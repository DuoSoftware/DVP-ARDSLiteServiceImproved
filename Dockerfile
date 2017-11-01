##FROM ubuntu
#RUN apt-get update
#RUN apt-get install -y git nodejs npm
#RUN git clone https://github.com/DuoSoftware/DVP-ARDSLiteServiceImproved.git /usr/local/src/ardsliteserviceimproved
#RUN cd /usr/local/src/ardsliteserviceimproved; npm install
#CMD ["nodejs", "/usr/local/src/ardsliteserviceimproved/app.js"]

#EXPOSE 8828

FROM node:5.10.0
ARG VERSION_TAG
RUN git clone https://github.com/DuoSoftware/DVP-ARDSLiteServiceImproved.git /usr/local/src/ardsliteserviceimproved
RUN cd /usr/local/src/ardsliteserviceimproved;
WORKDIR /usr/local/src/ardsliteserviceimproved
RUN npm install
EXPOSE 8828
CMD [ "node", "/usr/local/src/ardsliteserviceimproved/app.js" ]
