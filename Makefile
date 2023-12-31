build:
	docker-compose build
down:
	docker-compose down --volumes --remove-orphans
run:
	make down && docker-compose up
run-scaled:
	make down && docker-compose up -d --scale worker=$(worker)
stop:
	docker-compose stop