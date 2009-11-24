require File.dirname(__FILE__) + '/test_helper'

module DatabaseTestHelper
  def db
    @db
  end

  def test_set_and_get
    assert_equal nil, db['foo']
    assert_equal [],  db.get('foo')

    db.set('foo', [1,2,3])
    assert_equal [1,2,3], db['foo']
    assert_equal [[1,2,3]], db.get('foo')
  end

  def test_set_and_get_with_tuples
    assert_equal [],  db.get(['foo', 1])
    assert_equal [],  db.get(['foo', 2])

    db.set(['foo', 1], [1,2,3])
    assert_equal [[1,2,3]], db.get(['foo', 1])

    db.set(['foo', 2], [3,4,5])
    assert_equal [[3,4,5]], db.get(['foo', 2])

    assert_equal [[1,2,3], [3,4,5]], db.get('foo', :partial => true)
  end

  def test_get_with_ranges
    100.times do |i|
      db.set([:foo, i], {:id => i, :type => 'foo'})
    end

    assert_equal (17..34).to_a, db.get([:foo, 17]..[:foo, 34]).collect {|r| r[:id]}
  end
end
