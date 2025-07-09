from setuptools import setup, find_packages

setup(
    name='privacy-guardian-dataflow',
    version='1.0.0',
    description='Privacy Guardian Agent with Cloud Dataflow integration',
    packages=['privacy_guardian'],
    install_requires=[
        'apache-beam[gcp]>=2.40.0',
        'google-cloud-dlp>=3.0.0',
        'google-cloud-bigquery>=3.0.0',
        'pandas>=1.3.0',
        'google-adk>=0.1.0',
    ],
    python_requires='>=3.8',
)
