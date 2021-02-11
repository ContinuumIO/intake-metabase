from setuptools import setup
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = [
    "intake",
    "pandas",
    "requests",
]

setup(
    name='intake-metabase',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Metabase driver for Intake",
    long_description=long_description,
    long_description_content_type='text/markdown',
    license="BSD",
    author="Albert DeFusco",
    author_email='adefusco@anaconda.com',
    url='https://github.com/ContinuumIO/intake-metabase',
    packages=['intake_metabase'],
    entry_points={
        'intake.drivers': [
            'metabase_catalog = intake_metabase.source:MetabaseCatalog',
            'metabase_table = intake_metabase.source:MetabaseTableSource',
            'metabase_question = intake_metabase.source:MetabaseQuestionSource'
        ]
    },
    install_requires=requirements,
    keywords='intake-metabase',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
