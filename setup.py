from setuptools import find_packages, setup
from distutils.core import setup


setup(
    name='nats_repository',
    packages=find_packages(),
    version='0.0.1',
    license='MIT',
    description='',
    author='Syed Hassaan Saleem',
    author_email='saleemhassaan94@gmail.com',
    url='',
    download_url='',
    keywords=[
        'NATS Repository'
    ],
    install_requires=[
        'nats-py==2.9.0'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8'
    ]
)
