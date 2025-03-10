mkdir -p ../utils_layer/python/utils 
mkdir -p ../packages/utils
cp -r ../utils/ ../utils_layer/python/
cd ../utils_layer
zip ../packages/utils/utils.zip -r python/
