from typing import Optional, Union

import numpy as np
import pandas as pd
from caveclient import CAVEclient
from loguru import logger
from nglui import parser


class AnnotationBroadcaster:
    def __init__(
        self,
        caveclient: CAVEclient,
        broadcast_mapping: dict,
    ):
        """Broadcast annotations from one table to another based on a mapping

        Parameters
        ----------
        caveclient : CAVEclient
            Initialized CAVEclient object.
        broadcast_table : str
            Name of the CAVE table to broadcast annotations to.
        broadcast_mapping : dict
            Mapping of CAVE table names to a list of columns to broadcast.
        """
        self.caveclient = caveclient
        self.broadcast_mapping = broadcast_mapping

    def parse_state(self, state: dict, anno_layers: Optional[Union[str, list[str]]] = None):
        if anno_layers is None:
            anno_layers = parser.annotation_layers(state)
        elif isinstance(anno_layers, str):
            anno_layers = [anno_layers]
        tags = {}
        for ln in anno_layers:
            tags[ln] = parser.tag_dictionary(state, ln)
        anno_df = parser.annotation_dataframe(state)

        # Filter out annotations not in selected layers
        anno_df = anno_df.query("layer in @anno_layers").reset_index(drop=True)
        anno_df["num_tags"] = anno_df["tags"].apply(len)

        # Add boolean columns to each tag
        dfs = []
        for ln in anno_layers:
            df_ln = anno_df.query("layer == @ln").reset_index(drop=True).explode("tags")
            with pd.option_context("future.no_silent_downcasting", True):
                df_ln["tag_value"] = df_ln["tags"].replace(tags[ln]).astype(object)
            dfs.append(df_ln)
        anno_df = pd.concat(dfs, ignore_index=True)
        return anno_df

    def get_state(
        self,
        state_url: Union[str, int],
    ):
        "Split out in case we want to add more functionality later."
        if isinstance(state_url, str):
            state_id = int(state_url.split("/")[-1])
        else:
            state_id = state_url
        return self.caveclient.state.get_state_json(state_id)

    def state_annotations(self, state_url: Union[str, int], anno_layers: Optional[Union[str, list[str]]] = None):
        """Get an annotation dataframe from a state URL or state ID.

        Parameters
        ----------
        state_url : Union[str, int]
            URL to a json state on a CAVE state service.
        anno_layers : Optional[Union[str, list[str]]], optional
            List of annotation layers to use , by default None

        Returns
        -------
        pd.DataFrame
            DataFrame with annotations as rows and boolean columns for each tag.
        """
        state = self.get_state(state_url)
        return self.parse_state(state, anno_layers)

    def generate_cave_annotations(self, state_df):
        "Generate a list of annotations from a dataframe of annotations."
        anno_list = {}
        for table_name, anno_mapping in self.broadcast_mapping.items():
            anno_list[table_name] = []
            stage = self.caveclient.annotation.stage_annotations(table_name)
            skipped_rows = 0
            for layer_name, layer_mapping in anno_mapping.items():
                rel_df = state_df.query(f"layer == '{layer_name}'")
                for _, row in rel_df.iterrows():
                    anno = process_row(row, layer_mapping)
                    if anno:
                        stage.add(**anno)
                    else:
                        skipped_rows += 1
            anno_list[table_name].extend(stage.annotation_list)
            logger.info(
                f"Generated {len(anno_list[table_name])} annotations in layer {layer_name} for table {table_name}"
            )
            logger.info(f"Skipped {skipped_rows} annotations in layer {layer_name} for table {table_name}")
        return anno_list

    def broadcast_annotations(
        self,
        state_url: Union[str, int],
        anno_layers: Optional[Union[str, list[str]]] = None,
        *,
        dry_run=False,
    ):
        """_summary_

        Parameters
        ----------
        state_url : Union[str, int]
            URL to a json state on a CAVE state service, or just the state ID.
        anno_layers : Optional[Union[str, list[str]]], optional
            Specific set of annotation layers to use, by default None.
        dry_run : bool, optional
            If True, process all steps except final upload, by default False

        Returns
        -------
        new_annotation_ids : dict
            Dictionary with table names as keys and lists of new annotation IDs as values.
            If a dry run, the values will be negative integers.
        """
        state_df = self.state_annotations(state_url, anno_layers=anno_layers)
        anno_list = self.generate_cave_annotations(state_df)
        new_anno_ids = {}
        for table_name, annos in anno_list.items():
            if dry_run:
                logger.info(f"DRY RUN: Would upload {len(annos)} annotations to {table_name}")
                new_anno_ids[table_name] = [-1 - x for x in range(len(annos))]
            else:
                new_anno_ids[table_name] = self.caveclient.annotation.post_annotation(table_name, annos)
                logger.success(f"Uploaded {len(anno_list)} annotations to {table_name}")
        return new_anno_ids


def process_row(row, mapping):
    "Make a point annotation from a row in a dataframe."
    anno = {}
    for key, value in mapping.items():
        if value.get("type") == "point":
            # field mapping for points
            if np.any(pd.isna(row["point"])):
                return None
            anno[key] = row["point"]
        elif value.get("type") == "description":
            # field mapping for description
            if pd.isna(row["description"]):
                return None
            anno[key] = row["description"]
        elif value.get("type") == "value":
            anno[key] = value.get("value")
        elif value.get("type") == "tag":
            # field mapping for tag values
            tag_values = value.get("limit_to", None)
            if tag_values:
                if row["tag_value"] not in tag_values:
                    return None
            elif pd.isna(row["tag_value"]):
                return None
            elif value.get("single_tag", False) and row["num_tags"] > 1:
                return None
            anno[key] = row["tag_value"]
        elif value.get("type") == "reference_target":
            # Reference target ids expected to be in the description.
            # The key in this case should be "target_id"
            anno[key] = int(row["description"])
        else:
            msg = f"Unknown annotation type: {value.get('type')}"
            raise ValueError(msg)
    return anno
