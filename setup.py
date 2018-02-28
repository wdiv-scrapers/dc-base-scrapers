from setuptools import setup

setup(
    name='dc_base_scrapers',
    author="chris48s",
    license="MIT",
    url="https://github.com/wdiv-scrapers/dc-base-scrapers/",
    packages=['dc_base_scrapers'],
    install_requires=[
        'lxml==3.6.4',
        'retry==0.9.2',
        'scraperwiki==0.5.1',
        'arcgis2geojson==1.1.1',
        'commitment==2.0.0',
    ],
)
