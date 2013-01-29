require 'spec_helper'

describe "When two or more relationships share the same property" do 
  before do
    class Book < ActiveFedora::Base 
      has_many :collections, :property=>:is_part_of, :class_name=>'Collection'
      has_many :people, :property=>:is_part_of#, :class_name=>'Person'
    end

    class Person < ActiveFedora::Base
      belongs_to :book, :property=>:is_part_of
    end

    class Collection < ActiveFedora::Base
      belongs_to :book, :property=>:is_part_of
    end

    @book = Book.create!#(:collections=>[@collection1, @collection2], :people=>[@person1, @person2])
    @person1 = Person.create!(:book=>@book)
    @person2 = Person.create!(:book=>@book)
  end
  after do
      Object.send(:remove_const, :Collection)
      Object.send(:remove_const, :Person)
      Object.send(:remove_const, :Book)
  end

  it "Should only return relationships of the correct class" do
    @book.reload
    @book.people.should == [@person1, @person2]
    @book.collections.should == []
  end

  it "should not try to update relationships  on objects that are already deleted" do
    class Item < ActiveFedora::Base
      has_many :components, :property => :is_part_of
    end
    class Component < ActiveFedora::Base
      belongs_to :item, :property => :is_part_of
    end

    i = Item.create
    c = Component.create
    i.components << c
    i.save!
    c.delete
    i.delete # Causes an update of `c' which results in an error.
  end
end
