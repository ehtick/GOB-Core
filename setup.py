import os
from setuptools import setup, find_packages

setup(
    name='gobcore',
    version='0.1',
    description='GOB Core Components',
    url='https://github.com/Amsterdam/GOB-Core',
    author='Datapunt',
    author_email='',
    license='MPL-2.0',
    install_requires=[
        f'GDAL=={os.environ["LIBGDAL_VERSION"]}',
        'cryptography==3.3.2',
        'cx-Oracle==7.3.0',
        'datapunt-objectstore==2020.9.7',
        'geoalchemy2',
        'geomet',
        'ijson==2.3',
        'pandas==1.1.4',
        'paramiko==2.7.1',
        'pika==0.12.0',
        'psycopg2-binary==2.8.6',
        'pycryptodome==3.9.4',
        'pyjwt==1.7.1',
        'pyodbc==4.0.30',
        'requests==2.20.0',
        'shapely==1.7.1',
        'sqlalchemy==1.3.3',
        'xlrd==1.2.0',
    ],
    packages=find_packages(exclude=['tests*']),
    dependency_links=[
        'git+https://github.com/Amsterdam/objectstore.git@v1.0#egg=datapunt-objectstore',
    ],
)
