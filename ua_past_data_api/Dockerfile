FROM gcr.io/gcer-public/googlecloudrunner
RUN ["install2.r", "bigrquery", "googleAnalyticsR", "googleAuthR", "googleCloudRunner"]
COPY ["./", "./"]
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb(commandArgs()[4]);pr$run(host='0.0.0.0', port=as.numeric(Sys.getenv('PORT')))"]
CMD ["api.R"]
