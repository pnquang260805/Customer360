ARG IMAGE_NAME

FROM ${IMAGE_NAME}

COPY ./bash/mongo-setup.sh .
RUN chmod +x ./mongo-setup.sh
CMD [ "./mongo-setup.sh" ]