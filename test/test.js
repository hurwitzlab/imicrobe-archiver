var assert = require('assert');

let chai = require('chai');
let chaiHttp = require('chai-http');
let should = chai.should();

var config = require('../config.json');

chai.use(chaiHttp);

describe('Array', function() {
  describe('#indexOf()', function() {
    it('should return -1 when the value is not present', function() {
      assert.equal([1,2,3].indexOf(4), -1);
    });
  });
});

describe('Jobs', () => {
//    beforeEach((done) => { //Before each test we empty the database
//        Book.remove({}, (err) => {
//           done();
//        });
//    });

/*
  * Test the /GET route
  */
  describe('/GET jobs', () => {
      it('should GET all the jobs', (done) => {
        chai.request('http://localhost:3008')
            .get('/jobs')
            .set('Authorization', 'Bearer ' + config.testToken)
            .end((err, res) => {
                res.should.have.status(200);
                res.body.should.be.a('array');
                res.body.length.should.be.eql(0);
              done();
            });
      });
  });

});

