from setuptools import setup, find_packages

setup(
    name='pymydump',
    author='Anton Aksola',
    author_email='aakso@iki.fi',
    license='Apache License, Version 2.0',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
    ],
    entry_points=dict(
        console_scripts=[
            'pymydump=pymydump.cmd.main:main'
        ]
    ))