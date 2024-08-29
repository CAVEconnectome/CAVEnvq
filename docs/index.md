# CAVE-NeuvueQueue Integration

The `cavenvq` package is designed to bridge two systems used for dealing with proofreading and analysis of large 3d EM volumes.
CAVE is a system designed to handle tracking arbitrary annotations across proofreading, while Neuvue is designed to manage a broad collection of proofreading tasks.

Integration is designed such that CAVE is the first place proofreading tasks are generated and the ultimate place where task status is recorded.
The `cavenvq` system handles synchronized distribution of CAVE tasks.
Tasks are uploaded to CAVE and sent to Neuvue in parallel.
As proofreading occurs, one can run code here to update the status of specified CAVE tables to reflect the completion of various subtasks.
Finally, when a subtask is finished, pre-defined rules can specify new annotations to generate.

## CAVE and Neuvue

CAVE tables track arbitrary annotations.
The main level of organization in CAVE is the table, which is a collection of annotations following the same schema, and the annotation, which is a row in the table.
Annotations are typically associated with a point in space to allow tracking of objects across proofreading.

The main organizing level for tasks in Neuvue is the **namespace**, which is a collection of tasks that are related in some way and share basic properties like dataset.
The person who creates a task is its **author** and the proofreader/s who will work on it are the **assignees**.
Tasks have a **status** that can be updated as they are worked on and submitted and a **priority** that can be used to sort tasks.
The neuroglancer state is saved across time and the final state can be parsed to get information about what the proofreader did by the end of the task.

## CAVE Tables

Before getting started, one needs to create a linked pair of CAVE tables.
The first table can be thought of as the **Task Table**.
Each entry of this table corresponds to a single proofreading task, which can have one or multiple subtasks, and the ID of a given entry is unique within the table (i.e. this is the Task ID).
The combination of the name of the Task Table and the annotation ID is thus unique and will remain constant across proofreading.
The exact schema of the Task Table is flexible and might differ for different task categories depending on what information a manager wants to track or keep handy for reference.
A basic example might be a `bound_tag` which has a spatial point and a string field ("tag").
The spatial point could point to the soma of the neuron to be proofread, for example, and the string field could refer to the Neuvue "namespace" used to manage subtasks.
Together, these could be used to quickly visualize finished and unfinished neurons and make it easy to look up related tasks in neuvue.
Ultimately, however, any such values are for book-keeping and not explicitly used in the computational pipelines.

The second table is the **Task Status Table**, and must be a reference table for the Task Table.
Entries in the Task Status Table point to specific tasks in the Task Table by Task ID and have a field describing the status.
The main reason to separate the status from the main task table is that this allows us to update the status as much as we want without changing the unique annotation ID.
The status is only updated when all current subtasks are finished.
The Task Status schema should be a `reference_tag`, which basically amounts to a single text string and a target task ID.

Individually, the data uploaded would be something like:

**Task Table**

| id | pt_position | tag |
| :---: | :---: | :---: |
| 1 | [1,2,3] | my_neuvue_namespace |
| 2 | [4,5,6] | my_neuvue_namespace |
| 3 | [7,8,9] | my_neuvue_namespace |

and

**Task Status Table**

| target_id | tag (status) |
| :---: | :---: |
| 1 | finished |
| 2 | given |
| 3 | given |

However, when querying the Status table these are joined together on task ID and neuron segmentation IDs are looked up, giving:

**Task Tables merged on Task Status**

| id | pt_position | pt_root_id | tag | tag (status) |
| :---: | :---: | :---: | :---: | :---: |
| 1 | [1,2,3] | 123123123 | my_neuvue_namespace | finished |
| 2 | [4,5,6] | 456456456 | my_neuvue_namespace | given |
| 3 | [7,8,9] | 789789789 | my_neuvue_namespace | given |

### Table creation 

To fasciliate creating the pair of tables, the `cavenvq.create_task_tables` function will simplify the process and offer some sensible defaults.
You pass it a table name and description for the Task Table, and it will create the Task Table and Task Status Table with the simple schema: A "bound tag" for the Task Table (with a spatial point and associated root id as well as a single text field) and a reference "tag" for the Task Status Table (a single text field).

For example, to create a table called `my_nvq_tasks` with a description of "A table to track my proofreading tasks", you would run:

```python
import cavenvq
import caveclient as CAVEclient

client = CAVEclient( "MY_DATASTACK_NAME" ) # Replace with your datastack name
cavenvq.create_task_tables(
    table_name = "my_nvq_tasks",
    description = "A table to track my proofreading tasks",
)
```

and it would create tables `my_nvq_tasks` and `my_nvq_tasks_status`.

### Creating and distributing tasks

Using CAVEnvq to distribute tasks has three steps:

1. Create individual `Tasks` that contain information about what is to be proofread and what should happen after the task is finished.
2. Assemble tasks into a `TaskList` that has information about where to place tasks in CAVE and Nuevue.
3. Upload the TaskList to CAVE and Neuvue.

**Creating Tasks**

Each Task is made up of one or more Neuroglancer states, with each state corresponding to a single Nuevue subtask, as well as a variety of metadata to explain what the task is and how to update tables once it is marked as complete.
Each Task will have one row in the Task Table on CAVE.

To create a task, you need to provide a `Task` object with the following fields:

- `states`: One or more links to JSON states that form the initial state for proofreading. Why might we want more than one subtask per task? This is useful when you want to break up a large proofreading task into smaller pieces that can be done in smaller boutes, for example working on different axon branches of the same neuron.
 (Required)
- `represtative_point` : A point in the volume that is useful for visualizing the task, such as a cell body position. Only used for 
visualization, not any of the work itself. (Required if the schema includes a point)
- `annotation_kws`: A dictionary of key-value pairs to be used for the Task Table CAVE annotation, in addition to the representative point. For example, this could be `{"tag": "my_neuvue_namespace"}` if you want to add the namespace in the tag field. (Required as necessary make the annotation match the schema of the Task Table)
- `instructions` : A string describing teh task for the proofreading, for example: "Clean up the dendrite". (optional)
- `point_field` : The field in the Task Table that holds the representative point. Can be omitted if the field is named `"pt_position"`, just as with the default bound tag. (optional)
- `assignees` : A list of Nuevue usernames or queues to assign the task to. (optional)
- `seg_id` : The root ID of the neuron to be proofread. Can be used for tracking, but is not used for any computation. (optional)
- `priority` : An integer priority for the task. Higher numbers are higher priority. (optional)
- `existing_task` : Set to True if the task already exists. (optional)
- `id` : The annotation ID for a preexisting task, otherwise a new one is created. Only use if you are updating an existing task. (optional)


A simple task might look like the following, given a neuroglancer state `ngl_state` and a 3-d location `soma_point` and using the default schemas desribed above.

```python
task = cavenvq.Task(
    states=ngl_state,
    representative_point=soma_point,
    annotation_kws={'tag': 'my_neuvue_namespace'},
)
```

**Assembling TaskLists**

The TaskList collects Tasks and carries a description about how to handle them in CAVE and Neuvue.
Bulk values for several Task properties can also be used to set defaults for all tasks in the list, although the values in the Task object will override these defaults.

To create a TaskList, you need to provide a `TaskList` object with the following fields:

- `tasks`: A list of `Task` objects to be uploaded. (Required)
- `cave_task_table`: The name of the Task Table in CAVE to upload to. (Required)
- `namespace` : The name of the Neuvue namespace to upload to. (Required)
- `author`: The Neuvue username of the person who created the task. (Required)
- `next_status`: The name of the status to update the Task Status to when its subtasks are finished. (Required)
- `new_tasks` : Set True if the tasks are new, False if they are updates from a previously existing collection. This would be False if, for example, tasks correspond to individual cells and this TaskList is the second (or more) go-through of the same cells. (Required)

Optional TaskList fields are:

- `initial_status`: The status to assign to the tasks when they are first uploaded. (default: "given")
- `cave_status_table`: The name of the Task Status Table in CAVE to upload to. (default: cave_task_table + `"_status"`)
- `status_field`: The field in the Task Status Table that holds the status. Can be omitted if the field is named `"tag"`, just as with the default bound tag. (default: "tag")
- `representative_point_resolution`: The resolution of the representative point in nanometers. If none is given, it defaults to the value in the status table.
- `cave_broadcast_mapping`: A dictionary mapping the final neuroglancer state of each subtask to a set of *new* annotations

Optional TaskList fields that set default values for all tasks in the list are as follows. In all cases, they just set the default value for the corresponding field in the Task object.

- `assignees`
- `priority`
- `annotation_kws`
- `task_instruction`

**Uploading TaskLists**

Tasklists are uploaded to CAVE and Neuvue in a single step through a TaskPublisher object that gets a CAVEclient and a Neuvue client.
To ensure there are no errors, the TaskList is validated before uploading, and a dry_run can be used to check that the TaskList is valid and working as expected.

```python
taskpub = cavenvq.TaskPublisher(
    client = client,
    nvq_client = nvq_client,
)

# To do a dry run:
taskpub.dry_run( tasklist )

# To upload:
taskpub.publish_tasks(tasklist)
```

Logs will show the annotations being uploaded to CAVE and Neuvue.

## Updating Task Status

When a Task is complete finished, we want to update the status in the CAVE table.
Thankfully, when we generated tasks we used the Neuvue metadata to add all of the information needed to update the original task.
To check if a task is finished, we can use the `QueueReader` object to check the status of all tasks in a namespace.

```python
reader = cavenvq.QueueReader(
    caveclient = caveclient,
    nvq_client = nvq_client,
    namespace = "my_neuvue_namespace",
)

# Dry run to check status without updating
reader.run_update(dry_run=True)

# Process update
reader.run_update()
```

This will check *all* tasks in the namespace and update the status of the corresponding CAVE table entries when all subtasks are finished.
It will also mark these tasks as processed in Neuvue, so they will not be updated again.
Note that the `run_update` method works at the namespace level and does not need specific task IDs.

The complete table information can also be queried using `reader.get_task_data()`.
This will return a pandas DataFrame with all Neuvue rows in the namespace, including the status of the task, its processing status and all metadata.

**Config files**

To make it easier to automate these processes, you can use a config file to store the information needed to connect to CAVE and Neuvue.
The config file is a TOML file and can be written with the TaskPublisher object when saving out the TaskList originially.

```python
# To write a config file:
taskpub.write_config( tasklist, "my_config.toml" )

# To read a config file into a reader
reader = QueueReader.from_config( config_file = "my_config.toml" )
```

## Generating New Annotations from Finished Tasks

When a task is finished, we can generate new annotations based on the final state of the proofreading.
This is done by specifying a `cave_broadcast_mapping` in the TaskList.

This mapping is a dictionary that relates annotations in a neuroglancer state to CAVE annotations.
The first two levels in the dictionary specify the CAVE table name to add annotations to and the neuroglancer layer to take annotations from, respectively.
The third level species the name of each field in the CAVE table schema, and the forth level gives rules for how to do the mapping.

```python

mapping = {
    TARGET_TABLE_NAME : {
        NEUROGLANCER_LAYER_NAME: {
            field_name_1 : { 'type': mapping_type, ...}, 
            field_name_2 : ...,
        }
    }
}
```

There are a small number of mapping types that can be used.

- `point` : The value is a point in space associated with each annotation in the layer.
- `description` : The value is a string description of the annotation.
- `tag` : The value is a string associated with the annotation tag. Optionally, a list of strings can be provided under the `limit_to` key to only add annotations from a specific subset of tags. This could be useful if you want to omit, say, "unclear" style annotations.
Note that Neuroglancer allows more than one tag per annotation, and the current implementation will create one annotation per tag per point by default. For example, two tags on the same point will create two annotations in the CAVE table at the same location.
To supress this behavior, set the `single_tag` key to `True` in the tag mapping, but note that in this case neither tag will be added from multitag annotations.
- `reference_target` : If the broadcast table is a reference table, this will expect the `description` field to be an integer id for the target of the reference. Currently, all reference tables use the field name `target_id` for this, but you must specify this field name in the mapping anyway. Because this uses the `description` field, it is not possible to use this alongside the `description` mapping type nor should the descriptions be editted in Neuroglancer. You can fill the description fields from `nglui` by using the `description_column` parameter of the annotation mapper.
- `value` : The value is a fixed value, given under the `value` key.

For example, let's say we have a CAVE table called `my_new_annotations` that uses the `cell_type_local` schema with the fields `pt_position`, `cell_type` (a string), and `classification_system` (another string).
We have a neuroglancer layer called `annotations` with the tags `excitatory`, `inhibitory`, and `unknown` and a point associated with a collection of neurons.
The following mapping would map cell type annotations to the `cell_type` field and assign a fixed value "CellClass" to the `classification_system` field.

```python

mapping = {
    "my_new_annotations" : {
        "annotations": {
            "pt_position" : { 'type': 'point' },
            "cell_type" : { 'type': 'tag', 'limit_to': ['excitatory', 'inhibitory'], 'single_tag': True },
            "classification_system" : { 'type': 'value', 'value': 'CellClass' },
        }
    }
}
```

It is important that you make sure the schema of the CAVE table matches your mapping and the table already exists.