## Finastra solution

Get started on Unix:
1) Configure the app with app.properties (if needed)
2) Run websocket server image
3) cd to project root folder
4) `docker build -t stream-processor-ilya .`
5) `docker run --rm -it --network host  -v "$(pwd)"/data:/data stream-processor-ilya`


This will launch stream processor container.
You will see the progress in console. 
Once done, the result will be saved to _data_ folder.
