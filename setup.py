from setuptools import setup

with open("README.md", "r") as fh:
      long_description = fh.read()

setup(name='gobcore',
      version='0.1',
      description='GOB Core Components',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url='https://github.com/Amsterdam/GOB-Core',
      author='Datapunt',
      author_email='',
      license='MIT',
      packages=['gobcore'],
      classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
      ],
      zip_safe=False)
