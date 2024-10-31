from setuptools import find_packages, setup

setup(
    name="ap98j3envoubi3fco1kc",
    version="0.0.82",
    packages=find_packages(),
    install_requires=["lxml>=4.9.3", "wordsegment==1.3.1", "exorde_data", "aiohttp", "python-dotenv", "pytz"],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
