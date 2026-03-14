
FROM python:3.11

WORKDIR	/workspace

RUN	apt-get	update	&&	\
	apt-get	install	-y	\
		btop	\
		vim	\
		nano	\
		procps	\
		iproute2	&&	\
	rm	-rf	/var/lib/apt/lists/*

COPY requirements.txt .
RUN	pip	install	--no-cache-dir -r requirements.txt
RUN	rm requirements.txt

RUN	groupadd --gid 3000 bae4969
RUN	useradd --uid 1000 --gid bae4969 --shell /bin/bash --create-home bae4969
RUN	chown -R bae4969:bae4969 /workspace
USER bae4969

