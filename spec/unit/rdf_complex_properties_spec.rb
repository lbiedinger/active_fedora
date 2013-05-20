require 'spec_helper'

describe ActiveFedora::RDFDatastream do
  describe "a new instance" do
    its(:metadata?) { should be_true}
    its(:content_changed?) { should be_false}
  end
  describe "an instance that exists in the datastore, but hasn't been loaded" do
    before do 
      class DummyMADS < RDF::Vocabulary("http://www.loc.gov/mads/rdf/v1#")
        property :Topic
        property :elementList
        property :elementValue
        property :TopicElement
        property :authoritativeLabel
        class Topic
          include ActiveFedora::RdfObject
          def default_write_point_for_values 
            [:elementList, :topicElement]
          end
          
          # rdf_type DummyMADS.Topic
          map_predicates do |map|
            map.elementList(in: DummyMADS, to: "elementList", class_name:"DummyMADS::ElementList")
            map.computed!(:authoritativeLabel, in: DummyMADS, sources: [:elementList], separator: ", ")
          end
        end
        class ElementList
          include ActiveFedora::RdfObject
          def default_write_point_for_values 
            [:elementValue]
          end
          rdf_type DummyMADS.elementList
          map_predicates do |map|
            map.topicElement(in: DummyMADS, to: "TopicElement")
            map.elementValue(in: DummyMADS)
          end
        end
      end
      class ComplexRDFDatastream < ActiveFedora::NtriplesRDFDatastream
        map_predicates do |map|
          map.topic(in: DummyMADS, to: "Topic", class_name:"DummyMADS::Topic")
          map.title(in: RDF::DC)
        end
      end
      @ds = ComplexRDFDatastream.new(stub('inner object', :pid=>'foo', :new? =>true), 'descMetadata')
    end
    after do
      Object.send(:remove_const, :ComplexRDFDatastream)
      Object.send(:remove_const, :DummyMADS)
    end
    subject { @ds } 
    
    describe "complex properties" do
      it "should support assignment operator, assertion operator, and computed properties" do
        # @ds.topic.build
        # @ds.topic.first.elementList.build
        # @ds.topic[0].elementList[0].topicElement = "Cosmology"
        # puts @ds.graph.dump(:rdfxml)
        # puts @ds.graph.dump(:ntriples)
        @ds.topic = ["Cosmology"]
        @ds.topic << "Quantum States"
        @ds.topic.should == ["Cosmology", "Quantum States"]
        @ds.topic(0).elementList(0).topicElement.should == ["Cosmology"] 
        # @ds.topic(0).authoritativeLabel.should == "Cosmology"
        
        simplified_expected_xml = '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:mads="http://www.loc.gov/mads/rdf/v1#">
           <rdf:Description rdf:about="info:fedora/foo">
           <mads:Topic>
          <mads:authoritativeLabel>Cosmology</mads:authoritativeLabel>
          <mads:elementList rdf:parseType="Collection">
            <mads:TopicElement>
              <mads:elementValue>Cosmology</mads:elementValue>
            </mads:TopicElement>
          </mads:elementList>
        </mads:Topic>
        <mads:Topic>
          <mads:authoritativeLabel>Quantum States</mads:authoritativeLabel>
          <mads:elementList rdf:parseType="Collection">
            <mads:TopicElement>
              <mads:elementValue>Quantum States</mads:elementValue>
            </mads:TopicElement>
          </mads:elementList>
        </mads:Topic>
        </rdf:Description>
        </rdf:RDF>'
        
        list1_id = @ds.topic(0).elementList(0).rdf_subject.id
        list2_id = @ds.topic(1).elementList(0).rdf_subject.id
        
        expected_xml = '<?xml version="1.0" encoding="UTF-8"?>
               <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ns0="http://www.loc.gov/mads/rdf/v1#">
                 <rdf:Description rdf:about="info:fedora/foo">
                   <ns0:Topic>
                     <rdf:Description>
                       <ns0:elementList rdf:nodeID="'+list1_id+'"/>
                     </rdf:Description>
                   </ns0:Topic>
                   <ns0:Topic>
                     <rdf:Description>
                       <ns0:elementList rdf:nodeID="'+list2_id+'"/>
                     </rdf:Description>
                   </ns0:Topic>
                 </rdf:Description>
                 <ns0:elementList rdf:nodeID="'+list1_id+'">
                   <ns0:TopicElement>Cosmology</ns0:TopicElement>
                 </ns0:elementList>
                 <ns0:elementList rdf:nodeID="'+list2_id+'">
                   <ns0:TopicElement>Quantum States</ns0:TopicElement>
                 </ns0:elementList>
               </rdf:RDF>'
        
        @ds.graph.dump(:rdfxml).should be_equivalent_to expected_xml
      end
    end

  end
end
