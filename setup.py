from setuptools import setup, find_packages

setup(
    name="analytics-work",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    description="Analytics tools and utilities",
    author="Izzaz Iskandar",
    author_email="izzaz@time.com.my",
    url="https://github.com/izzaziii/analytics-work",
    python_requires=">=3.8",
    install_requires=[
        "numpy",
        "pandas",
        "matplotlib",
        "seaborn",
        "scikit-learn",
        "jupyter",
        "python-dotenv",
    ],
    # Define the actual package name
    py_modules=["analytics"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
