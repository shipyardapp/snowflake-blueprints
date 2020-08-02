from pathlib import Path
from pkg_resources import parse_requirements
from setuptools import find_packages, setup


for path in Path('./').rglob('requirements.txt'):
    with Path(path).open() as requirements_txt:
        install_requires = [
            str(requirement)
            for requirement
            in parse_requirements(requirements_txt)
        ]


config = {
    "description": "Simplified data pipeline blueprints for working with Snowflake.",
    "author": "Shipyard Team",
    "url": "https: // www.shipyardapp.com",
    "author_email": "tech@shipyardapp.com",
    "packages": find_packages(),
    "install_requires": install_requires,
    "name": "snowflake-blueprints",
    "version": "v0.1.0",
    "license": "Apache-2.0",
    "classifiers": [
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Other Audience",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    "python_requires": ">=3.7"}

setup(**config)
