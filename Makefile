build:
	docker-compose build
down:
	docker-compose down --volumes --remove-orphans
run:
	make down && docker-compose up
stop:
	docker-compose stop