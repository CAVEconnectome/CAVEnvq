# Example

In this example, we will create a set of proofreading tasks where we ask proofreaders to assign the category of the postsynaptic targets of a particular neuron.
Note that this assumes you are already familiar with and authenticated into CAVEclient and Neuvue.

First, we need to decide on a neuron and the categories we want to use.
Here, will will use pick an inhibitory neuron and the categories will be "excitatory", "inhibitory", "unsure", and "error."

#### Initialize CAVE tables and Neuvue namespaces

You will only need to do this once for each table you want to use for tracking proofreading, but it should be done first.

For the CAVE tables, we can use the `cavenvq.create_task_tables` function to create the necessary task tables.
We will use the default schema, since they offer everything we need.
In addition, because we want to create new annotations from the work done by proofreaders, we also need to define a "broadcast" table where these can live.
Here, will make these a new table called `test_synapse_target_broadcast` and set up with a "bound tag" that will just have a single string value.
Scientifically, a reference annotation would make more sense but here we want to use the point annotations.

```python
import cavenvq
from neuvueclient import NeuvueQueue
from caveclient import CAVEclient

datastack = 'minnie65_phase3_v1'
caveclient = CAVEclient(datastack)

# Set up CAVE task table
task_table = "test_synapse_target_proofreading"
cavenvq.tables.create_task_tables(
    table_name = task_table,
    description = "Table to test cavenvq for tracking proofreading of synapse targets. By Casey Schneider-Mizell",
    voxel_resolution = [4,4,40],
    cv_client = caveclient,
)

# Set up the broadcast table for annotations created during proofreading.
# This will be a standard table, so set it up as usual for CAVE.
broadcast_table = "test_synapse_target_broadcast"


# Set up Neuvue namespace
nv_namespace = 'cavenvq_test'


```

For Neuvue, we will need to create a namespace for the proofreading tasks.
This is done by going to the [Neuvue web interface](https://app.neuvue.io), going to *Admin Tools*>*Console* and then clicking on *Namespaces*.
From there, click "ADD NAMESPACE" in the upper right and set up the namespace you want to use.
For this example, our namespace will be `cavenvq_test`.

#### Get a collection of synapses and build up neuroglancer states

We will get a list of inhibitory neurons and then build up a neuroglancer state for a random sample of synapses from each one and add a particular set of tags that we will want proofreaders to use.
This is just CAVE client stuff.
Note that the links we want are directly to the JSON, and do not include the neuroglancer viewer URL.

```python
from nglui import statebuilder

cell_df = caveclient.materialize.tables.allen_column_mtypes_v2(classification_system='inhibitory').query()

# Get three random cells with cell type labeled "DTC"
root_ids = cell_df.query('cell_type=="DTC"').sample(3)['pt_root_id'].values

# Make a statebuilder for each synapse state we will give to proofreaders
TAGS = ['excitatory', 'inhibitory', 'unsure', 'error']
img, seg = statebuilder.from_client(caveclient)
seg.add_selection_map(selected_ids_column='pre_pt_root_id')

anno = statebuilder.AnnotationLayerConfig(
    name='synapses',
    mapping_rules=statebuilder.PointMapper(point_column='post_pt_position', linked_segmentation_column='post_pt_root_id'),
    tags=TAGS,
)

sb = statebuilder.StateBuilder([img, seg, anno], client=caveclient)

# Generate JSON states for each Task. Here, we will just do one state per Task, and we will also get a representative point for convenience.
syn_df = caveclient.materialize.synapse_query(pre_ids=root_ids)
state_urls = []
rep_points = []
for rid in root_ids:
    syns = syn_df.query('pre_pt_root_id==@rid').sample(50)
    rep_points.append(syns['pre_pt_position'].iloc[0])
    state_dict = sb.render_state(syns, return_as='dict')
    state_id = caveclient.state.upload_state_json(state_dict)
    state_urls.append(
        caveclient.state.build_neuroglancer_url(state_id).split('=')[-1]
    ) # Carve off direct link to JSON file
```

The resulting state urls will be something like:

```python
['https://global.daf-apis.com/nglstate/api/v1/5437284948115456',
 'https://global.daf-apis.com/nglstate/api/v1/5465241561333760',
 'https://global.daf-apis.com/nglstate/api/v1/5221012541014016']
```

#### Create and submit tasks

Now that we have tables made and a set of states, we can finally use cavenvq to submit them to Neuvue and CAVE.

First we make the Tasks:

```python
tasks = []
for rep_pt, state in zip(rep_points, state_urls):
    tasks.append(
        cavenvq.Task(
            states=state,
            representative_point=rep_pt,
            annotation_kws={'tag': nv_namespace}, # Fill out the tag field of the task annotation for CAVE
        )
    )
```

Next, we assemble them into a TaskList.
Here, we want to also define a broadcast mapping to generate a new set of annotations from the proofreading work.
Since this will just be a table with a `bound_tag` schema, we only need to set up `pt_position` and `tag` fields.

```python
broadcast_mapping = {
    broadcast_table: {
        "synapses": {
            "tag": {"type": "tag", "limit_to": ["excitatory", "inhibitory"]},
            "pt_position": {"type": "point"},
        }
    }
}

tasklist = cavenvq.TaskList(
    tasks=tasks,
    cave_task_table=task_table,
    namespace=nv_namespace,
    broadcast_mapping=broadcast_mapping,
    task_instructions="Please assign the category of the postsynaptic targets of the synapses.",
    initial_status="given",
    next_status="completed",
    author="casey",
    assignees="casey",
    new_tasks=True,
)
```

We can now submit the tasks to Neuvue and CAVE.

```python
nvq_url = 'https://queue.neuvue.io'
nv_client = NeuvueQueue(nvq_url)

td = cavenvq.TaskPublisher(
    caveclient=caveclient,
    nv_client=nv_client,
)

td.publish_tasks(tasklist)
# Or use td.dry_run(tasklist) to see what would happen without actually submitting the tasks
```

It will also be convenient to save a config file to make it more convenient to check the queue later.

```python
td.save_task_config(tasklist, 'synapse_task_config.toml')
```

#### Checking the queue

At this point, the assigned proofreaders can go to the Neuvue web interface and start proofreading.
We will now want to check the status of tasks and update the CAVE tables with the results when they are finished.

The verbose version of this would take all of the parameters we set above.

```python
import cavenvq
from neuvueclient import NeuvueQueue
from caveclient import CAVEclient

datastack = 'minnie65_phase3_v1'
caveclient = CAVEclient(datastack)

nvq_url = 'https://queue.neuvue.io'
nv_client = NeuvueQueue(nvq_url)
nv_namespace = 'cavenvq_test'

qr = cavenvq.QueueReader(
    caveclient=caveclient,
    nv_client=nv_client,
    nv_namespace=nv_namespace,
)
```

Alternatively, we can just load the config file we saved earlier.

```python
import cavenvq
qr = cavenvq.QueueReader.from_config('synapse_task_config.toml')
```

And then to check the status of tasks and update the CAVE tables:

```python
qr.run_update()
# To see what would happen without actually updating the tables, we can run `qr.run_update(dry_run=True)` instead
```

This will update anything in the namespace, not only those cells in the TaskList we had made.
Becuase every Neuvue subtask has sufficient information to update CAVE tables, we don't need to specify the TaskList again.

To check the status of the tasks, we can use the `qr.get_task_data()` to get a DataFrame of the all Neuvue tasks and their status.
