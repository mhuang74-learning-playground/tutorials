'use strict';

const express = require('express');
const app = express();

const simpleOauthModule = require('simple-oauth2');

const oauth2_github = simpleOauthModule.create({
  client: {
    id: '5c9e56be55066fdb8705',
    secret: '78236fb7c079e6bbbbd1d1e99798c4502614742d',
  },
  auth: {
    tokenHost: 'https://github.com',
    tokenPath: '/login/oauth/access_token',
    authorizePath: '/login/oauth/authorize',
  },
});



// Authorization uri definition
const authorizationUri_github = oauth2_github.authorizationCode.authorizeURL({
  redirect_uri: 'http://localhost:3000/callback_github',
  scope: 'notifications',
  state: '3(#0/!~',
});

// Initial page redirecting to Github
app.get('/auth_github', (req, res) => {
  console.log(authorizationUri_github);
  res.redirect(authorizationUri_github);
});

// Callback service parsing the authorization token and asking for the access token
app.get('/callback_github', (req, res) => {
  const code = req.query.code;
  const options = {
    code,
  };

  oauth2_github.authorizationCode.getToken(options, (error, result) => {
    if (error) {
      console.error('Access Token Error', error.message);
      return res.json('Authentication failed');
    }

    console.log('The resulting token: ', result);
    const token = oauth2_github.accessToken.create(result);

    return res
      .status(200)
      .json(token);
  });
});

var passport = require('passport-debug');
var PinterestStrategy = require('passport-pinterest').Strategy;

passport.use('pinterest', new PinterestStrategy({
      clientID: '1447547',
      clientSecret: 'd911fad6fd3bd5f942f0938516e51231b58b450f',
      scope: ['read_public', 'read_relationships'],
      callbackURL: "https://localhost:3000/callback/pinterest",
      state: true
  },
  function(accessToken, refreshToken, profile, done) {
    console.log('Pinterest refresh token: ', refreshToken);
    console.log('Pinterest access token: ', accessToken);
  }
));

// set up Passport for Express
var session = require("express-session"),
    bodyParser = require("body-parser");

app.use(express.static("public"));
app.use(session({ secret: "cats" }));
app.use(bodyParser.urlencoded({ extended: false }));
app.use(passport.initialize());
app.use(passport.session());

// Initial page redirecting to Pinterest
app.get('/auth/pinterest', (req, res) => {
  console.log('trying to authenticate with Pinterest..');
  passport.authenticate('pinterest');
  // console.log(req.get('Authorization'));
  // next();
});

// Callback service parsing the authorization token and asking for the access token
app.get('/callback/pinterest', (req, res) => {

  passport.authenticate('pinterest', { failureRedirect: '/' }),
  function(req, res) {
      // Successful authentication, redirect home. 
      res.redirect('/success');
  }

});

app.get('/success', (req, res) => {
  res.send('');
});

app.get('/', (req, res) => {
  res.send('Hello Sir!<br><br><br><a href="/auth_github">Log in with Github</a><br><a href="/auth/pinterest">Log in with Pinterest</a>');
});

app.listen(3000, () => {
  console.log('Express server started on port 3000'); // eslint-disable-line
});


// Credits to [@lazybean](https://github.com/lazybean)
