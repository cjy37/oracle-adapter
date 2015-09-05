var adapter = require('../../dist/adapter'),
  should = require('should'),
  support = require('./support/bootstrap');

describe('adapter', function() {

  /**
   * Setup and Teardown
   */

  before(function(done) {
    support.Setup('test_destroy', done);
  });

  after(function(done) {
    support.Teardown('test_destroy', done);
  });

  /**
   * DESTROY
   *
   * Remove a row from a table
   */

  describe('.destroy()', function() {

    describe('with options', function() {

      before(function(done) {
        support.Seed('test_destroy', done);
      });

      it('should destroy the record', function(done) {
        adapter.destroy('test', 'test_destroy', {
          where: {
            id: 1
          }
        }, function(err, result) {

          // Check record was actually removed
          support.Client(function(err, client) {
            client.execute('SELECT * FROM "test_destroy"', [], (err, result) => {

              // Test no rows are returned
              result.rows.length.should.eql(0);

              // close client
              client.release((err) => {
                if (err) console.log('problem releasign connection', err)
              })

              done();
            });
          });

        });
      });

    });
  });
});
