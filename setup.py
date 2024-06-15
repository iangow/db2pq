import setuptools

with open("README.md", "r") as f:
    long_description = f.read()
    print(long_description)

setuptools.setup(
    name="db2pq",
    version="0.0.6",
    author="Ian Gow",
    author_email="iandgow@gmail.com",
    description="Convert database tables to parquet tables.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/iangow/db2pq/",
    packages=setuptools.find_packages(),
    install_requires=['ibis-framework[duckdb, postgres]', 'pyarrow', 'paramiko'],
    python_requires=">=3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
