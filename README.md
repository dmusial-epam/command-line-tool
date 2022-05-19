## Build docker
```
docker build -t <name> .
```
## Run command program with volume
```
docker run -v <absolute_path>/:/scripts <name> python main.py --help
```
## or 
```
docker run -v <absolute_path>/:/scripts <name> python main.py <command> --help
```
