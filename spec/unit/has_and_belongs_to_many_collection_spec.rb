require 'spec_helper'

describe ActiveFedora::Associations::HasAndBelongsToManyAssociation do
  before do 
    class Book < ActiveFedora::Base
    end
    class Page < ActiveFedora::Base
    end
  end

  after do
    Object.send(:remove_const, :Book)
    Object.send(:remove_const, :Page)
  end

  it "should call add_relationship" do
    subject = Book.new(pid: 'subject:a')
    subject.stub(:new_record? => false, save: true)
    predicate = Book.create_reflection(:has_and_belongs_to_many, 'pages', {:property=>'predicate'}, nil)
    ActiveFedora::SolrService.stub(:query).and_return([])
    ac = ActiveFedora::Associations::HasAndBelongsToManyAssociation.new(subject, predicate)
    ac.should_receive(:callback).twice
    object = Page.new(:pid => 'object:b')
    object.stub(:new_record? => false, save: true, id: '1234')
  
    subject.should_receive(:[]=).with('pages_ids', '1234')
 
    ac << object

  end

  it "should call add_relationship on subject and object when inverse_of given" do
    subject = Book.new(pid: 'subject:a')
    subject.stub(:new_record? => false, save: true)
    predicate = Book.create_reflection(:has_and_belongs_to_many, 'pages', {:property=>'predicate', :inverse_of => 'inverse_predicate'}, nil)
    ActiveFedora::SolrService.stub(:query).and_return([])
    ac = ActiveFedora::Associations::HasAndBelongsToManyAssociation.new(subject, predicate)
    ac.should_receive(:callback).twice
    object = Page.new(:pid => 'object:b')
    object.stub(:new_record? => false, save: true)
  
    subject.should_receive(:add_relationship).with('predicate', object)
 
    object.should_receive(:add_relationship).with('inverse_predicate', subject)
 
    ac << object

  end

  it "should call solr query multiple times" do

    subject = Book.new(pid: 'subject:a')
    subject.stub(:new_record? => false, save: true)
    predicate = Book.create_reflection(:has_and_belongs_to_many, 'pages', {:property=>'predicate', :solr_page_size => 10}, nil)
    ids = []
    0.upto(15) {|i| ids << i.to_s}
    query1 = ids.slice(0,10).map {|i| "_query_:\"{!raw f=id}#{i}\""}.join(" OR ")
    query2 = ids.slice(10,10).map {|i| "_query_:\"{!raw f=id}#{i}\""}.join(" OR ")
    subject.should_receive(:ids_for_outbound).and_return(ids)
    ActiveFedora::SolrService.should_receive(:query).with(query1, {:rows=>10}).and_return([])
    ActiveFedora::SolrService.should_receive(:query).with(query2, {:rows=>10}).and_return([])

    ac = ActiveFedora::Associations::HasAndBelongsToManyAssociation.new(subject, predicate)
    ac.find_target
  end

  context "class with association" do
    before do
      class Collection < ActiveFedora::Base
        has_and_belongs_to_many :members, :property => :has_collection_member, :class_name => "ActiveFedora::Base" , :after_remove => :remove_member
        def remove_member (m)
        end
      end

      class Thing < ActiveFedora::Base
        has_many :collections, property: :has_collection_member, :class_name => "ActiveFedora::Base"
      end
    end

    after do
      Collection.destroy_all
      Thing.destroy_all
      Object.send(:remove_const, :Collection)
      Object.send(:remove_const, :Thing)
    end

    let(:thing) {Thing.create()}
    let(:collection) {Collection.create().tap {|c| c.members << thing}}

    it "should call destroy" do
      expect(collection.destroy).to_not raise_error
    end

  end

end
