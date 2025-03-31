from setuptools import setup, find_packages

setup(
    name="cc-store",
    version="0.1.0",
    description="High-performance storage system for Common Crawl data",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "pyarrow>=3.0.0",
        "fsspec>=2021.6.0",
        "s3fs>=2021.6.0",
        "mmh3>=3.0.0",  # MurmurHash for content hashing
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "black>=21.5b2",
            "pylint>=2.8.0",
        ],
    },
    python_requires=">=3.7",
) 