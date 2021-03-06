from setuptools import setup

setup(
    name='dc_base_scrapers',
    author="chris48s",
    license="MIT",
    url="https://github.com/wdiv-scrapers/dc-base-scrapers/",
    packages=['dc_base_scrapers'],
    install_requires=[
        'arcgis2geojson>=2,<3',
        'commitment>=2,<3',
        'geojson-rewind==0.2.0',
        'lxml>=4.2,<5',
        'retry==0.9.2',
        'scraperwiki==0.5.1',
    ],
)
