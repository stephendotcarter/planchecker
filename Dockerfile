FROM golang:onbuild
RUN mkdir /app
ADD . /app/ 
WORKDIR /app  
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM scratch
WORKDIR /root/
COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=0 /app .
CMD ["./app"]
