const assert = require('assert');

let chai = require('chai');
let chaiHttp = require('chai-http');
let should = chai.should();

chai.use(chaiHttp);

const config = require('../config.json');
const hostUrl = 'http://localhost:' + config.serverPort;

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

  describe('/GET jobs', () => {
      it('should GET all the jobs', (done) => {
        chai.request(hostUrl)
            .get('/jobs')
            .set('Authorization', 'Bearer ' + config.agaveConfig.testToken)
            .end((err, res) => {
                res.should.have.status(200);
                res.body.should.be.a('array');
                //res.body.length.should.be.eql(0);
              done();
            });
      });
  });

  describe('/POST jobs', () => {
      it('should POST a new job', (done) => {
        chai.request(hostUrl)
            .post('/jobs')
            .set('Authorization', 'Bearer ' + config.agaveConfig.testToken)
            .send({
                projectId: 285
            })
            .end((err, res) => {
                res.should.have.status(200);
              done();
            });
      });
  });

});

