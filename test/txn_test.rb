require 'test_helper'

class TxnTest < Test::Unit::TestCase

  # cTxnStat = rb_define_class_under(mBdb,"TxnStat",rb_cObject);
  # rb_define_method(cTxnStat,"[]",stat_aref,1);
  # 
  # cTxnStatActive =
  #   rb_define_class_under(cTxnStat,"Active",rb_cObject);
  # rb_define_method(cTxnStatActive,"[]",stat_aref,1);
  # 
  # cTxn = rb_define_class_under(mBdb,"Txn",rb_cObject);
  # rb_define_method(cTxn,"commit",txn_commit,1);
  # rb_define_method(cTxn,"abort",txn_abort,0);
  # rb_define_method(cTxn,"discard",txn_discard,0);
  # rb_define_method(cTxn,"tid",txn_id,0);
  # rb_define_method(cTxn,"set_timeout",txn_set_timeout,2);

  def test_x
    
  end
end
