build:
	docker-compose build
down:
	docker-compose down --volumes --remove-orphans
run:
	make down && docker-compose up --scale worker=3
stop:
	docker-compose stop