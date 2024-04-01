import attrs
from .utils import TaskValidationError, flatten_list, status_table_name
from typing import Optional, Union
from caveclient import CAVEclient
from neuvueclient import NeuvueQueue


@attrs.define
class Task:
    """
    Represents the configuration for a single proofreading task with one or more neuroglancer states.


    Parameters
    ----------
    states : list
        A list of neuroglancer states.
    representative_point : list
        A single point in 3D space that represents the task.
    instructions : str
        The instructions for the task.
    id : Optional[int]
        The ID of the task (default: None).
    annotation_kws : Optional[dict]
        Additional keyword arguments for the task annotations in CAVE (default: None).
    point_field : str
        Name of the bound spatial point field in the CAVE table (default: "pt_root_id").
    assignees : Optional[Union[str, list[str]]]
        Who the task is assigned to (default: None).
    seg_id : Optional[int]
        The ID of the segment (default: None).
    priority : Optional[int]
        The priority of the task (default: None).
    """

    states: list
    representative_point: list
    instructions = str
    id: Optional[int] = None
    annotation_kws: Optional[dict] = None
    point_field: str = "pt_position"
    assignees: Optional[Union[str, list[str]]] = None
    seg_id: Optional[int] = None
    priority: Optional[int] = None

    @property
    def as_new_annotation(self):
        anno = {
            self.point_field: self.representative_point,
        }
        anno.update(self.annotation_kws)
        return anno

    def set_id(self, new_id):
        self.id = new_id

    def __attrs_post_init__(self):
        if self.annotation_kws is None:
            self.annotation_kws = {}


@attrs.define
class TaskList:
    """
    Represents a list of proofreading tasks with associated configurations.

    Parameters
    ----------
    tasks : list[Task]
        The list of tasks, using the TaskConfig defined above.
    cave_task_table : str
        The name of the CAVE task table.
    namespace : str
        The Neuvue namespace.
    author : str
        The Neuvue author.
    next_status : str
        The status that the task will change to after proofreading.
    new_tasks : bool
        Indicates whether the task entry is new or already existing.
    initial_status : Optional[str], optional
        The initial status, if tasks are new. Defaults to "given".
    assignees : Optional[str], optional
        The assignees. Defaults to None.
    cave_status_table : Optional[str], optional
        The CAVE table for tracking the status of tasks. Defaults to None, which uses utils.status_table_name
    cave_broadcast_table : Optional[str], optional
        The CAVE table to broadcast finished annotatoins to. Defaults to None.
    cave_broadcast_mapping : Optional[dict], optional
        Rules for broadcasting finished annotations. Defaults to None.
    task_instruction : Optional[dict], optional
        The task instruction. Defaults to None.
    representative_point_resolution : Optional[list], optional
        The representative point resolution. Defaults to None.
    annotation_kws : Optional[dict], optional
        Additional CAVE annotation keywords for new tasks. Defaults to None.
    status_field : Optional[str], optional
        The column name for the proofreading status. Defaults to "tag".
    priority : Optional[int], optional
        Task priority across all tasks. Defaults to 1.
    """

    tasks: list[Task]
    cave_task_table: str
    namespace: str
    author: str
    next_status: str
    new_tasks: bool
    initial_status: Optional[str] = "given"
    assignees: Optional[str] = None
    cave_status_table: Optional[str] = None
    cave_broadcast_table: Optional[str] = None
    cave_broadcast_mapping: Optional[dict] = None
    task_instruction: Optional[dict] = None
    representative_point_resolution: Optional[list] = None
    annotation_kws: Optional[dict] = None
    status_field: Optional[str] = "tag"
    priority: Optional[int] = 1

    def initial_status_annotation(self, task_id):
        anno = {
            "target_id": task_id,
            "status_field": self.initial_status,
        }
        return anno

    def neuvue_metadata(self, task):
        meta = {
            "task_id": task.id,
            "cave_task_table": self.cave_task_table,
            "cave_status_table": self.cave_status_table,
            "cave_broadcast_table": self.cave_broadcast_table,
            "cave_broadcast_mapping": self.cave_broadcast_mapping,
            "next_status": self.next_status,
        }
        return meta

    def task_to_neuvue_list(self, task):
        return [
            {
                "author": self.author,
                "assignees": self.task_assignee(task),
                "instructions": self.neuvue_instructions(task),
                "namespace": self.namespace,
                "metadata": self.neuvue_metadata,
                "ng_state": state,
                "seg_id": task.seg_id,
                "priority": self.task_priority(task),
            }
            for state in task.states
        ]

    def neuvue_tasks(self):
        return flatten_list([self.task_to_neuvue_list(task) for task in self.tasks])

    def neuvue_instructions(self, task):
        return {"prompt": task.instructions}

    def task_priority(self, task, fallback_priority=1):
        pr = None
        if task.priority:
            pr = task.priority
        elif self.priority:
            pr = self.priority
        else:
            pr = fallback_priority
        return pr

    def validate_tasks(self):
        if self.new_tasks is False:
            for task in self.tasks:
                if task.id is None:
                    msg = "All tasks must have existing ids if new_tasks is False"
                    raise TaskValidationError(msg)
        if self.namespace is None:
            msg = "Namespace must be set"
            raise TaskValidationError(msg)
        if self.author is None:
            msg = "Author must be set"
            raise TaskValidationError(msg)
        for task in self.tasks:
            self.task_assignee(task)

    def task_assignee(self, task):
        assignees = self.assignees
        if task.assignees:
            assignees = task.assignees
        if isinstance(assignees, str):
            assignees = [assignees]
        if assignees is None:
            msg = "Assignees must be set either per task or globally"
            raise TaskValidationError(msg)
        return assignees

    def __attrs_post_init__(self):
        self.validate_tasks()
        if self.annotation_kws is None:
            self.annotation_kws = {}


class TaskDistributer:
    def __init__(
        self,
        tasklist: TaskList,
        caveclient: CAVEclient,
        nv_client: NeuvueQueue,
    ):
        """Distribute tasks to CAVE and Neuvue

        Parameters
        ----------
        tasklist : TaskList
            Configured TaskList object from above.
        caveclient : CAVEclient
            Initialized CAVEclient object.
        nv_client : NeuvueQueue
            Initialized NeuvueQueue object.
        """
        if tasklist.annotation_kws is not None:
            for task in tasklist.tasks:
                # Merge dictionaries, with the individual task winning ties.
                task.annotation_kws = tasklist.annotation_kws | task.annotation_kws
        self.tasklist = tasklist
        self.caveclient = caveclient
        self.nv_client = nv_client

    def _validate_cave_tables(self):
        table_list = self.caveclient.annotation.get_tables()
        if self.tasklist.cave_task_table not in table_list:
            raise TaskValidationError(f'Table "{self.tasklist.cave_task_table}" not in CAVE annotation tables')
        if self.tasklist.cave_status_table not in table_list:
            raise TaskValidationError(f'Table "{self.tasklist.cave_status_table}" not in CAVE annotation tables')
        pass

    def initialize_tasks(self):
        # Uploads the initial proofreading annotations and status annotations associated with them.
        # Also, populates a task id in each task.
        self._cave_annotations()

        # Broadcasts the various states to a neuvue queue task
        self._post_neuvue_tasks()

    def dry_run(self):
        # Return annotation dictionaries and neuvue task dictionaries without posting.
        pass

    def _cave_annotations(self, dry_run=False):
        stage = self._generate_initial_task_annotations(self)
        self._cave_task_annotations(stage)
        self._cave_status_annotations()
        return stage.annotation_

    def _generate_initial_task_annotations(self):
        # Uploads annotationss and stores the annotations id in the task
        stage = self.caveclient.annotation.stage_annotations(
            self.tasklist.cave_task_table, annotation_resolution=self.tasklist.representative_point_resolution
        )
        for task in self.tasklist.tasks:
            stage.add(**task.as_new_annotation)
        return stage

    def _cave_task_annotations(self, stage, dry_run=False):
        # Posts all annotations and stores the annotation id in
        if dry_run is not False:
            anno_ids = self.caveclient.annotation.upload_staged_annotations(stage)
        else:
            anno_ids = range(len(self.tasklist.tasks))
        for task, anno_id in zip(self.tasklist.tasks, anno_ids):
            task.set_id(anno_id)

    def _cave_status_annotations(self):
        stage = self.caveclient.annotation.stage_annotations(self.tasklist.cave_status_table)
        for task in self.tasklist:
            stage.add(**self.tasklist.initial_status_annotation(task.id))
        self.caveclient.annotation.upload_staged_annotations(stage)

    def _post_neuvue_tasks(self):
        nv_tasks = self.tasklist.neuvue_tasks()
        self._broadcast_neuvue_tasks(nv_tasks)

    def _generate_task_arguments(self):
        return []
