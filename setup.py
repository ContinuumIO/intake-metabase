from setuptools import setup
import versioneer

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
    license="BSD",
    author="Albert DeFusco",
    author_email='adefusco@anaconda.com',
    url='https://github.com/ContinuumIO/intake-metabase',
    packages=['intake_metabase'],
    entry_points={
        'intake.drivers': ['metabase_table = intake_metabase.source:MetabaseDatasetSource']
    },
    install_requires=requirements,
    keywords='intake-metabase',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ]
)
