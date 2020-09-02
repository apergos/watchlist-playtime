FROM ariel/devstretch:base

# set up apt properly
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update

RUN apt-get install -y autoconf automake git lintian bash-completion emacs
RUN apt-get install -y zip unzip debian-archive-keyring ubuntu-archive-keyring gcc g++ flex make libtool


EXPOSE 22
CMD /usr/sbin/sshd -D
