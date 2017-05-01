FROM armhf/debian

RUN set -ex \
        && export DEBIAN_FRONTEND='noninteractive' \
        && apt-get update && apt-get install -y --no-install-recommends --no-install-suggests ca-certificates \
          python3 python3-pip \
          python3-lxml \
        && rm -rf /var/lib/apt/lists/* \

        && apt-get clean -y && apt-get autoclean -y && apt-get autoremove -y \
        && rm -rf /var/cache/debconf/*-old /usr/share/doc/* /usr/share/man/* /usr/share/groff/* /usr/share/info/* /usr/share/locale/* /tmp/* /var/tmp/*

RUN set -ex \
        && export DEBIAN_FRONTEND='noninteractive' \
        && apt-get update && apt-get install -y --no-install-recommends --no-install-suggests \
          git build-essential \
        && rm -rf /var/lib/apt/lists/* \

        && apt-get clean -y && apt-get autoclean -y && apt-get autoremove -y \
        && rm -rf /var/cache/debconf/*-old /usr/share/doc/* /usr/share/man/* /usr/share/groff/* /usr/share/info/* /usr/share/locale/* /tmp/* /var/tmp/*


RUN set -ex \
	&& pip3 install \
    requests cssselect

COPY go1.8.1.linux-armv6l.tar.gz /tmp/go1.8.1.linux-armv6l.tar.gz
RUN set -ex \
	&& tar -C /usr/local -xzf /tmp/go1.8.1.linux-armv6l.tar.gz \
  && echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc \
  && /usr/local/go/bin/go get github.com/mattn/go-sqlite3
