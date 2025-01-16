VU Distributed Systems - External Scheduler for Ray
====================================================

*Group Members:* Ingmar Biemond (2725600), Simon Bezemer (2731853), Maurits Reijnaert (2739377)

Project
-------
The goal of this project is to design an external scheduler as a service to work with Ray.

The scheduler is located at `./external_scheduler/scheduler.py <./external_scheduler/scheduler.py>`__.

Modifications have also been made to the Ray source code in order to allow it to support communication with the external scheduler service.

The current communication protocol between Ray and the scheduler is as follows:

.. list-table::
   :widths: 25 50 25 25
   :header-rows: 1

   * - Function
     - Message code
     - Message content
     - Reply
   * - add node
     - ``0x0``
     - ``[resources]``
     - ``0x0``
   * - remove node
     - ``0x1``
     - ``[8 bytes node ID]``
     - ``0x0``
   * - schedule
     - ``0x2``
     - ``[resources]``
     - *if successful:* ``0x0[8 bytes node ID]`` *, else:* ``[0x1]``

Where ``[resources]`` can be represented with any number of the following resource representation: ``[ASCII string resource key][0x0][8 bytes double resource value]``.

When communicating, each message is prepended with 8 bytes containing a 64-bit integer value representing the length of the following message, in bytes.

Communication to add a node would look like this, for example:
::
    <request>: [8 bytes length][0x0][8 bytes node ID][resource1][0x0][resource1 value][resource2][0x0][resource2 value]
      <reply>: [8 bytes length][0x0]

Installation
------------
In order to use our external scheduler, first build the project according to the `Building Ray from Source <https://docs.ray.io/en/latest/ray-contribute/development.html>`__ guide.
Specifically, follow the following steps among the ones listed there:
- `Fork and clone the repository <https://docs.ray.io/en/latest/ray-contribute/development.html#fork-the-ray-repository>`__
- `Prepare a Python virtual environment <https://docs.ray.io/en/latest/ray-contribute/development.html#prepare-a-python-virtual-environment>`__
- `Then, follow the steps to build Ray according to your OS <https://docs.ray.io/en/latest/ray-contribute/development.html#preparing-to-build-ray-on-linux>`__

Usage
-----
First, to use Ray with our external scheduler, start the external scheduler.

.. code-block:: bash

   # Cd into the cloned Ray repository:
   $ cd ray

   # Then, start the external scheduler:
   $ python external_scheduler/scheduler.py

   # If you need to define a different IP and/or port, use:
   $  python external_scheduler/scheduler.py -a <ip_address> -p <port>

By default, our external scheduler runs on IP ``127.0.0.1`` with port ``44444``. If multiple nodes are to be connected to the Ray instance, replace this with the current node's external IP address and ensure the port is correctly forwarded.

Then, in a different Terminal window, locate your project directory. In your current directory, from where you will be running your program, create a file named ``EXTERNAL_SCHEDULER_CONFIG.txt``. It will also be automatically created if it does not exist when Ray is initialized. This file is used to specify the IP and port used by the running external scheduler. By default, its contents will be:

.. code-block:: bash

   # EXTERNAL_SCHEDULER_CONFIG.txt
   127.0.0.1 44444

Change to match the IP and port used when running the external scheduler.

Lastly, in order to use Ray, two different approaches can be used. If you need just a local Ray instance to, for example, test program functionality, simply placing ``ray.init()`` in your program will start a local Ray instance, that will automatically connect to the external scheduler. However, this is usually limited to using just 4 CPU cores, instead of the full amount available in your system. If you need to fully utilize your CPU or connect other nodes to form a distributed system, it is possible to do that by first starting a Ray head-node instance, then connecting all worker nodes to that instance, and finally executing your program. It will automatically connect to a connected Ray instance if you use ``ray.init("auto")`` when initializing Ray.

To start a head-node, then connect other worker nodes, use:

.. code-block:: bash

   # On the head-node:
   $ ray start --head --port=6379

   # On the worker node(s):
   $ ray start --address='<head-node-ip>:6379'

Then, run the program. It should automatically connect to the Ray instance running on the machine. It might be necessary to wait for a while after connecting all nodes until the external scheduler shows no more new output, in order to ensure all nodes are correctly initialized. Running ``ray status`` can also check if all nodes are correctly connected to the Ray instance.

Benchmarks
----------
The following scripts were used for the benchmarks mentioned in our report:

- **Experiment 1**: `Monte Carlo Estimation of Pi <https://docs.ray.io/en/latest/ray-core/examples/monte_carlo_pi.html>`__
- **Experiment 2**: `Ray Torch Train <https://github.com/generalnobody/ray/blob/ray-2.39.0-dev/release/air_tests/air_benchmarks/workloads/torch_benchmark.py>`__
- **Experiment 3**: `XGBoost Train <https://github.com/generalnobody/ray/blob/ray-2.39.0-dev/release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py>`__

Report
------
The report can be found at `./report.pdf <./report.pdf>`__.


Original Ray README content
===========================

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://readthedocs.org/projects/ray/badge/?version=master
    :target: http://docs.ray.io/en/master/?badge=master

.. image:: https://img.shields.io/badge/Ray-Join%20Slack-blue
    :target: https://forms.gle/9TSdDYUgxYs8SA9e8

.. image:: https://img.shields.io/badge/Discuss-Ask%20Questions-blue
    :target: https://discuss.ray.io/

.. image:: https://img.shields.io/twitter/follow/raydistributed.svg?style=social&logo=twitter
    :target: https://twitter.com/raydistributed

.. image:: https://img.shields.io/badge/Get_started_for_free-3C8AE9?logo=data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8%2F9hAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAEKADAAQAAAABAAAAEAAAAAA0VXHyAAABKElEQVQ4Ea2TvWoCQRRGnWCVWChIIlikC9hpJdikSbGgaONbpAoY8gKBdAGfwkfwKQypLQ1sEGyMYhN1Pd%2B6A8PqwBZeOHt%2FvsvMnd3ZXBRFPQjBZ9K6OY8ZxF%2B0IYw9PW3qz8aY6lk92bZ%2BVqSI3oC9T7%2FyCVnrF1ngj93us%2B540sf5BrCDfw9b6jJ5lx%2FyjtGKBBXc3cnqx0INN4ImbI%2Bl%2BPnI8zWfFEr4chLLrWHCp9OO9j19Kbc91HX0zzzBO8EbLK2Iv4ZvNO3is3h6jb%2BCwO0iL8AaWqB7ILPTxq3kDypqvBuYuwswqo6wgYJbT8XxBPZ8KS1TepkFdC79TAHHce%2F7LbVioi3wEfTpmeKtPRGEeoldSP%2FOeoEftpP4BRbgXrYZefsAI%2BP9JU7ImyEAAAAASUVORK5CYII%3D
   :target: https://console.anyscale.com/register/ha?utm_source=github&utm_medium=ray_readme&utm_campaign=get_started_badge

Ray is a unified framework for scaling AI and Python applications. Ray consists of a core distributed runtime and a set of AI libraries for simplifying ML compute:

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/what-is-ray-padded.svg

..
  https://docs.google.com/drawings/d/1Pl8aCYOsZCo61cmp57c7Sja6HhIygGCvSZLi_AuBuqo/edit

Learn more about `Ray AI Libraries`_:

- `Data`_: Scalable Datasets for ML
- `Train`_: Distributed Training
- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `Serve`_: Scalable and Programmable Serving

Or more about `Ray Core`_ and its key abstractions:

- `Tasks`_: Stateless functions executed in the cluster.
- `Actors`_: Stateful worker processes created in the cluster.
- `Objects`_: Immutable values accessible across the cluster.

Learn more about Monitoring and Debugging:

- Monitor Ray apps and clusters with the `Ray Dashboard <https://docs.ray.io/en/latest/ray-core/ray-dashboard.html>`__.
- Debug Ray apps with the `Ray Distributed Debugger <https://docs.ray.io/en/latest/ray-observability/ray-distributed-debugger.html>`__.

Ray runs on any machine, cluster, cloud provider, and Kubernetes, and features a growing
`ecosystem of community integrations`_.

Install Ray with: ``pip install ray``. For nightly wheels, see the
`Installation page <https://docs.ray.io/en/latest/ray-overview/installation.html>`__.

.. _`Serve`: https://docs.ray.io/en/latest/serve/index.html
.. _`Data`: https://docs.ray.io/en/latest/data/dataset.html
.. _`Workflow`: https://docs.ray.io/en/latest/workflows/concepts.html
.. _`Train`: https://docs.ray.io/en/latest/train/train.html
.. _`Tune`: https://docs.ray.io/en/latest/tune/index.html
.. _`RLlib`: https://docs.ray.io/en/latest/rllib/index.html
.. _`ecosystem of community integrations`: https://docs.ray.io/en/latest/ray-overview/ray-libraries.html


Why Ray?
--------

Today's ML workloads are increasingly compute-intensive. As convenient as they are, single-node development environments such as your laptop cannot scale to meet these demands.

Ray is a unified way to scale Python and AI applications from a laptop to a cluster.

With Ray, you can seamlessly scale the same code from a laptop to a cluster. Ray is designed to be general-purpose, meaning that it can performantly run any kind of workload. If your application is written in Python, you can scale it with Ray, no other infrastructure required.

More Information
----------------

- `Documentation`_
- `Ray Architecture whitepaper`_
- `Exoshuffle: large-scale data shuffle in Ray`_
- `Ownership: a distributed futures system for fine-grained tasks`_
- `RLlib paper`_
- `Tune paper`_

*Older documents:*

- `Ray paper`_
- `Ray HotOS paper`_
- `Ray Architecture v1 whitepaper`_

.. _`Ray AI Libraries`: https://docs.ray.io/en/latest/ray-air/getting-started.html
.. _`Ray Core`: https://docs.ray.io/en/latest/ray-core/walkthrough.html
.. _`Tasks`: https://docs.ray.io/en/latest/ray-core/tasks.html
.. _`Actors`: https://docs.ray.io/en/latest/ray-core/actors.html
.. _`Objects`: https://docs.ray.io/en/latest/ray-core/objects.html
.. _`Documentation`: http://docs.ray.io/en/latest/index.html
.. _`Ray Architecture v1 whitepaper`: https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview
.. _`Ray Architecture whitepaper`: https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview
.. _`Exoshuffle: large-scale data shuffle in Ray`: https://arxiv.org/abs/2203.05072
.. _`Ownership: a distributed futures system for fine-grained tasks`: https://www.usenix.org/system/files/nsdi21-wang.pdf
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

Getting Involved
----------------

.. list-table::
   :widths: 25 50 25 25
   :header-rows: 1

   * - Platform
     - Purpose
     - Estimated Response Time
     - Support Level
   * - `Discourse Forum`_
     - For discussions about development and questions about usage.
     - < 1 day
     - Community
   * - `GitHub Issues`_
     - For reporting bugs and filing feature requests.
     - < 2 days
     - Ray OSS Team
   * - `Slack`_
     - For collaborating with other Ray users.
     - < 2 days
     - Community
   * - `StackOverflow`_
     - For asking questions about how to use Ray.
     - 3-5 days
     - Community
   * - `Meetup Group`_
     - For learning about Ray projects and best practices.
     - Monthly
     - Ray DevRel
   * - `Twitter`_
     - For staying up-to-date on new features.
     - Daily
     - Ray DevRel

.. _`Discourse Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Meetup Group`: https://www.meetup.com/Bay-Area-Ray-Meetup/
.. _`Twitter`: https://twitter.com/raydistributed
.. _`Slack`: https://www.ray.io/join-slack?utm_source=github&utm_medium=ray_readme&utm_campaign=getting_involved
