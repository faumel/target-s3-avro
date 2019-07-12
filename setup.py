#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3-avro",
    version="1.1.0",
    description="Singer.io target for extracting data into s3 stored as AVRO",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_s3_avro"],
    install_requires=[
        "singer-python>=5.0.12",
        "boto3>=1.4.1",
        "avro-python3>=1.9.0",
    ],
    entry_points="""
    [console_scripts]
    target-s3-avro=target_s3_avro:main
    """,
    packages=["target_s3_avro"],
    package_data = {},
    include_package_data=True,
)
