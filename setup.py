import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aio_statsd",
    version="0.2.1",
    author="so1n",
    author_email="so1n897046026@example.com",
    description="Simple and fast statsd async client",
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
