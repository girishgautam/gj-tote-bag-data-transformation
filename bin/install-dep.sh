pip install -r ../requirements-lambda.txt -t ../dependencies/python
mkdir -p ../packages/dependencies
cd ../dependencies
zip ../packages/dependencies/dependencies.zip -r python/
