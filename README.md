A service to handle publishing projects and data to the EBI and NCBI archives.

Setup
=====

Setup database:
```
cat db.sql | sqlite3 db.sqlite3
```

Modify configuration:

Edit config.json accordingly.

Running
=======
To try out:
```
npm install
npm start
```

For development:
```
npm install nodemon
nodemon server.js
```

For production:
```
sudo npm install pm2@latest -g
pm2 start --name imicrobe-archiver server.js
sudo pm2 startup systemd
```
