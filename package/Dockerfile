#
# build the target application
#
FROM golang:1.16.5-alpine3.13 as builder
RUN apk update && apk upgrade && apk add make gcc g++

WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
ARG GIT_COMMIT
RUN make rebuild-docker GIT_COMMIT="$GIT_COMMIT"

#
# build the target container
#
FROM alpine:3.13

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates curl && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup webservice && adduser webservice -G webservice -D

# set the timezone appropriately
ENV TZ=America/New_York
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME /ocr-ws
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME/bin
RUN chown -R webservice $APP_HOME && chgrp -R webservice $APP_HOME

# port and run command
EXPOSE 8088
CMD scripts/entry.sh

# Move in necessary assets
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh
COPY package/data/container_bash_profile /home/webservice/.profile
COPY --from=builder /build/bin/ocr-ws.linux $APP_HOME/bin/ocr-ws

# Ensure permissions are correct
RUN chown webservice:webservice /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/ocr-ws && chmod 755 /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/ocr-ws

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER webservice

#
# end of file
#
