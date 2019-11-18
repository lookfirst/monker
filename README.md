# The Money Maker Machine

This was a hobby project of myself to create the simplest Cryptocurrency bot
possible. It was written in Python, operates on Binance, and stores all logging
and trading information into a MongoDB database. The trading strategy is
extremely simple:

 - Assume that crypto prices will fluctuate, but eventually be restored to its
   original quote
 - Buy small quantities every time the price falls below a threshold, keeping
   track of the amount purchased and the price payed
 - Sell those exact quantities when the price is above what was payed for it

It was a lot of fun developing it, and I guess I made something around 3
dollars before shutting it down! :D

If you happen to use this I will be very happy. If you happen to make some
money out of this and want to contribute to my PhD studies, you can send
a donation to this bitcoin address: bc1quex38k9j044sfvfyrmggg7udzqr9zlx2lnd03p

# Installation Instructions

This instructions were tested on AWS with Ubuntu 18.04 LTS.

```
sudo apt install git python3-pip python3-virtualenv virtualenv mongodb
git clone git@github.com:khstangherlin/monker.git
cd monker
virtualenv -p /usr/bin/python3 tmp/monker
source tmp/monker/bin/activate.csh
pip install -r PRDREQ
```

# Run the Bot

Running the bot is quite straightforward. Make sure your API keys are not 
hardcoded together with the version controlled files. Use environment
variable scripts to set them:

```
export API_KEY="ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz"
export API_SECRET="5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx"

python monker.py
```

# Web Interface for Monitoring

There's been a bit of work to create a web interface with dashboard to monitor the
bot performance. It is not fully functional yet, but you are welcome to take a look
on the files `app.py` and `dash.html` and send in some contributions :)

My idea to keep the web interface simple and extremely secure, is to use no web
authentication system, but instead it would require an SSH connection and port forwarding
to the server machine.

