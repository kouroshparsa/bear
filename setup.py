"""
Bear
-----------
bear is a pipeline allowing you to run tasks in parallel in such
a way that it truely used multiple cores of your processor
Link
`````
* Source
  https://github.com/kouroshparsa/bear
"""
from setuptools import Command, setup, find_packages

version = '1.4'
import sys
setup(
    name='bear',
    version=version,
    url='https://github.com/kouroshparsa/bear',
    download_url='https://github.com/kouroshparsa/bear/packages/%s' % version,
    license='GNU',
    author='Kourosh Parsa',
    author_email="kouroshtheking@gmail.com",
    description='asynchronous parallelization pipeline',
    long_description=__doc__,
    packages=find_packages(),
    install_requires = ['psutil', 'matplotlib'],
    include_package_data=True,
    package_data = {'bear': []},
    zip_safe=False,
    platforms='all',
    classifiers=[
        'Programming Language :: Python',
    ]
)
