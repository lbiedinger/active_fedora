require 'spec_helper'

describe "Loading from the index, failing over to fedora" do
  before do
    class TestObj < ActiveFedora::Base
      has_metadata :name => "descMetadata", :type => Hydra::ModsArticleDatastream
      delegate_to :descMetadata, [:person, :abstract]
    end
    @obj = TestObj.create!(:person=>'my person', :abstract=>'my abstract')
    solr_doc = @obj.to_solr  #solr_doc = TestObj.find_with_conditions(:id => @obj.pid).first
  end

  after do
    @obj.destroy
  end
  describe "when the property is in solr (title)" do
    before do
      # show that title is in solr
      solr_doc = TestObj.find_with_conditions(:id => @obj.pid).first
      solr_doc['mods_person_tesim'].should == ['my person']
    end
    it "Should not hit fedora" do
      Rubydora::Repository.any_instance.should_not_receive(:datastream_dissemination).with(hash_including(pid: @obj.pid, dsid: 'descMetadata'))
      found = TestObj.find(@obj.pid)
      found.should be_loaded_from_cache
      found.person.should == ["my person"]
    end
  end

  describe "when the property is not in solr (abstract)" do
    before do
      # show that abstract is not in fact in solr
      solr_doc = TestObj.find_with_conditions(:id => @obj.pid)
      solr_doc.first.values.flatten.grep('my abstract').should be_empty
    end
    it "should load from fedora" do
      found = TestObj.find(@obj.pid)
      found.should be_loaded_from_cache
      # calling abstract needs to trigger the repository load
      found.abstract.should == ["my abstract"]
      found.should_not be_loaded_from_cache
    end
  end

  it "should load from fedora before an update"

end
