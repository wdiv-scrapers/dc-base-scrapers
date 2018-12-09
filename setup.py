from setuptools import setup

setup(
    name='dc_base_scrapers',
    author="chris48s",
    license="MIT",
    url="https://github.com/wdiv-scrapers/dc-base-scrapers/",
    packages=['dc_base_scrapers'],
    install_requires=[
        'arcgis2geojson>=1.3,<2',
        'commitment>=2,<3',
        'geojson-rewind==0.1.1',
        'lxml>=4.2,<5',
        'retry==0.9.2',
        'scraperwiki==0.5.1',
    ],
)
