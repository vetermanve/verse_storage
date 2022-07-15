FROM php:7.4
RUN apt-get update && apt-get install -y libpq-dev bash wget zip unzip && docker-php-ext-install pdo pdo_pgsql
ADD . /var/www
WORKDIR /var/www

# Installing Composer
RUN printf "\nInstalling Composer\n\n"; \
    EXPECTED_SIGNATURE=$(wget -q -O - https://composer.github.io/installer.sig); \
    php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"; \
    ACTUAL_SIGNATURE=$(php -r "echo hash_file('SHA384', 'composer-setup.php');"); \
    if [ "$EXPECTED_SIGNATURE" != "$ACTUAL_SIGNATURE" ]; \
    then \
      >&2 echo 'ERROR: Invalid installer signature'; \
      exit 1; \
    else \
      php composer-setup.php --install-dir=/usr/local/bin --filename=composer; \
    fi; \
    rm composer-setup.php

#
#--------------------------------------------------------------------------
# User www-data Configuration
#--------------------------------------------------------------------------
#
RUN cp -r /root/.composer /home/www-data/ \
    && chown -R www-data:www-data /home/www-data \
    && usermod -d /home/www-data www-data

ARG PUID=1000
ENV PUID ${PUID}
RUN if [ -n "${PUID}" ] && [ "${PUID%:*}" != 0 ]; then \
  usermod -u ${PUID} www-data >/dev/null 2>&1 \
;fi

ENTRYPOINT tail -f /dev/null