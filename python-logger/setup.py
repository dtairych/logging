# setup.py
from setuptools import setup, find_packages

setup(
    name="distributed-logger",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pika>=1.3.0",
        "typing-extensions>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.17.0",
            "black>=22.0.0",
            "mypy>=0.910",
        ]
    },
    python_requires=">=3.9",
    author="Your Name",
    author_email="your.email@example.com",
    description="A distributed logging system that integrates with RabbitMQ",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords="logging,rabbitmq,distributed-systems",
    url="https://github.com/yourusername/python-logger",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: System :: Logging",
    ],
)