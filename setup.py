from glob import glob

from os.path import basename
from os.path import splitext

from setuptools import find_packages, setup


setup(
    name="integratedexercise",
    version="0.0.1",
    description="Setup for integrated exercise",
    python_requires=">=3.9",
    packages=find_packages("src", exclude=["tests"]),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    keywords="data pipelines, data engineering",
)