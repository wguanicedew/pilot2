# PanDA Pilot - Architecture 2

## Contributions

1. Check the ``TODO.md`` and ``STYLEGUIDE.md`` files.

2. Fork the ``PanDAWMS/pilot2`` repository into your private account as ``origin``. Clone it and set the ``PanDAWMS/pilot2`` repository as ``upstream``.

3. Make new code contributions only to a new branch in your repository, push to ``origin`` and make a pull request into ``upstream``. Depending on the type of contribution this should go against either ``upstream/master``, ``upstream/main-dev`` or ``upstream/hotfix``.

## Verifying code correctness

Do not submit code that does not conform to the project standards. We use PEP8 and Flake verification, with everything enabled at a maximum line length of 160 characters and McCabe complexity 12:

    flake8 pilot.py pilot/

For Python 2.6 you need to install ``flake8<3.0.0``, which can miss a few things. Check the output of TravisCI to verify if you have to use this old version.

## Running the pilot

The pilot is a dependency-less Python application and relies on ``/usr/bin/env python``. The minimum pilot can be called like:

    ./pilot.py -d -q <QUEUE_NAME>

where ``QUEUE_NAME`` correspond to the ATLAS PandaQueue as defined in AGIS. This will launch the default ``generic`` workflow with lifetime default lifetime of 10 seconds (i.e., too short to do anything).

The ``-d`` argument changes the logger to produce debug output.

## Running the testcases

The test cases are implemented as standard Python unittests under directory ``pilot/test/``. They can be discovered and executed automatically:

    unit2 -v

## Building and viewing docs

1. Install ``sphinx`` into your environment by ``pip`` or other means with all the necessary requirements.

2. Navigate into ``./doc`` in your fork and run ``make html``.

3. Open ``_build/html/index.html`` with your browser.

### Automate documentation to your module

Add the following lines somewhere in the documentation:

    .. automodule:: your.module
        :members:

For more info, visit http://sphinx-doc.org
