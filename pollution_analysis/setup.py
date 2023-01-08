from setuptools import find_packages, setup


setup(
    name="pollution-analyisis",
    packages=find_packages(),
    version="0.1.0",
    install_requires=[
        "Click",
    ],
    entry_points="""
        [console_scripts]
        pollution-analyisis=main:cli
    """,
    description="""
        A project to analyse flaring and satellite imagery
    """,
    author="AQAI",
    license="",
)
