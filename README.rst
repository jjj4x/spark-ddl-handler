docker run --rm -it -p 5432:5432 --name postgres -e POSTGRES_PASSWORD=password postgres
docker image build --rm --tag spark-dll-handler:1.0.0
