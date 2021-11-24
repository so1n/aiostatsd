import setuptools  # type: ignore

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aio_statsd",
    version="0.2.7",
    author="so1n",
    author_email="so1n897046026@gmail.com",
    description="an asyncio-based client for send metric to StatsD, Graphite.carbon, TelegrafStatsD and DogStatsD.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/so1n/aiostatsd",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
