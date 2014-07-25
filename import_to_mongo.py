import os
import sys
import calendar
import json
import pprint
from datetime import datetime
from collections import defaultdict

from lxml import etree
from lxml.etree import XMLSyntaxError
import pymongo

from corenlp import *

# Required, found in https://github.com/dasmith/stanford-corenlp-python
from jsonrpc import ServerProxy,JsonRpc20,TransportTcpIp

MONTHS = {v:str(k).zfill(2) for k,v in enumerate(calendar.month_name)}
MONTHS[None] = None
MONGO = pymongo.MongoClient()
global DATABASE

#CORE_NLP = ServerProxy(JsonRpc20(), TransportTcpIp(
#                                        timeout=30, addr=('127.0.0.1', 8080)))
SPEAKER_SET = {}
WORD_SET = {}

pp = pprint.PrettyPrinter(indent=2)

#class CoreNLPServer():
#    def __init__(self,host="127.0.0.1",port=8080):
#        self.rpc_server = self.get_server()
#    def get_server(self):
#        return ServerProxy(JsonRpc20(), TransportTcpIp( addr=( 
#            self.host,self.port)))
#    def parse(self,string):
#        return self.rpc_server.parse(string)

class Word():
    def __init__(self,form,lemma,POS):
        self.form = form
        self.lemma = lemma
        self.POS = POS
        self.insert()
        #WORD_SET[(form,lemma,POS)] = self.word_id 
    def insert(self):
        result = MONGO[DATABASE].words.update(
                { 'word': self.form,'lemma' : self.lemma,'POS':self.POS },
                { 'word': self.form,'lemma' : self.lemma,'POS':self.POS },
                upsert=True)
        if result['updatedExisting']:
            obj = MONGO[DATABASE].words.find_one(
                { 'word': self.form,'lemma' : self.lemma,'POS':self.POS })
            self.word_id = obj['_id']
        else:
            self.word_id = result['upserted']

class ParsedSentence():
    def __init__(self,parse,turn_id,seq_no,speaker_id,document_id):
        #self.words = parse.pop('words')
        self.obj = parse
        self.obj['document_id'] = document_id
        self.obj['turn_id'] = turn_id
        self.obj['seq_no'] = seq_no
        self.obj['speaker_id'] = speaker_id
        self.make_words()
        self.obj['word_set'] = defaultdict(int)
        self.obj['word_count'] = self.count_words()
        self.edit_dependencies()
    def count_words(self):
        wc = 0
        for w in self.obj['words']:
            wc += 1
            self.obj['word_set'][str(w['word_id'])] += 1
        return wc
    def edit_dependencies(self):
        if self.obj['dependencies']:
            for dep in self.obj['dependencies']:
                d_i = int(dep['dependent']['index']) - 1
                g_i = int(dep['governor']['index']) - 1
                dep['dependent']['word_id'] = self.obj['words'][d_i]['word_id']
                dep['governor']['word_id'] = self.obj['words'][g_i]['word_id']
    def insert(self):
        pp.pprint(self.obj)
        self.obj['sentence_id'] = MONGO[DATABASE].sentences.insert(self.obj)
        return self.obj['sentence_id']
    def make_words(self):
        for word in self.obj['words']:
            word['word_id'] = Word(
                    word['word'],word['Lemma'],word['PartOfSpeech']).word_id
    def report_words(self):
        return [w['word_id'] for w in self.obj['words']]

class Turn():
    def __init__(self,seq_no,speaker,document_id):
        self.seq_no = seq_no
        self.sent_seq_no = 0
        self.text = ''
        self.speaker_id = speaker
        self.document_id = document_id
        self.sentence_ids = []
        self.word_count = 0
        self.word_set = defaultdict(int)
    def add_text(self,s):
        s = s.replace('\n',' ')
        self.text = self.text + ' ' + s
        self.parse_sentences(s)
    def parse_sentences(self,s):
        try:
            sentences = json.loads(CORE_NLP.parse(s))['sentences']
        except:
            print "PARSER ERROR"
            print "document:",self.document_id
            print "turn no:",self.seq_no
            print self.text
            raise
        for sentence in sentences:
            ps = ParsedSentence(
                    sentence,self.turn_id,self.sent_seq_no,self.speaker_id,self.document_id)
            sentence_id = ps.insert()
            self.sentence_ids.append(sentence_id)
            self.word_count += ps.obj['word_count']
            for k,v in ps.obj['word_set'].items():
                self.word_set[k] += v
            self.sent_seq_no += 1
    def insert(self):
        self.turn_id = MONGO[DATABASE].turns.insert({ 
                                            'seq_no'        : self.seq_no,
                                            'speaker'       : self.speaker_id,
                                            'text'          : self.text,
                                            'document_id'   : self.document_id})
        return self.turn_id
    def update(self):
        ret = MONGO[DATABASE].turns.update( {'_id':self.turn_id},
                {'$set': 
                    {   'sentence_ids'  : self.sentence_ids,
                        'word_set'      : self.word_set,
                        'word_count'    : self.word_count,
                        'text'          : self.text}})

class Speaker():
    def __init__(self,name):
        self.cr_name = name
        self.insert()
        # maybe add a lookup function here?
    def insert(self):
        result = MONGO[DATABASE].speakers.update(
                { 'CR_name' : self.cr_name },
                { 'CR_name' : self.cr_name },
                upsert=True)
        if result['updatedExisting']:
            obj = MONGO[DATABASE].speakers.find_one(
                    { 'CR_name' : self.cr_name })
            self.speaker_id = obj['_id']
        else:
            self.speaker_id = result['upserted']

class Document():
    def __init__(self,fname):
        # get etree from XML string
        self.x = etree.XML(open(fname).read())
        self.text = ''
        self.quotes = []
        # grab meta information from fixed fields
        self.get_meta_info()
        # look through for all speakers identified
        self.get_all_speakers()
        # insert what we have so far for this document, to get id for linking
        self.insert()
        self.turn_ids = []
        self.word_count = 0
        self.word_set = defaultdict(int)
        # iterate through speaking chunks and create turns and sentences,
        # accreting the document's text itself
        self.read_turns()
        #print worked
        #if worked:
        update_facts = self.update()
    def add_text(self,string):
        self.text = self.text + ' ' + string.replace('\n',' ')
    def add_turn(self,turn):
        self.turn_ids.append(turn.insert())
    def add_quote(self,quote_text,speaker,quote_index):
        self.quotes.append({'text':quote_text,
                            'speaker_id':self.meta_info['speakers'].get(speaker,None),
                            'index':quote_index})
    def update_turn(self,turn):
        turn.update()
        self.add_text(turn.text)
        self.word_count += turn.word_count
        for k,v in turn.word_set.items():
            self.word_set[k] += v
        turn.update()
    def read_turns(self):
        turn_seq_no = 0
        chunks = [c for c in self.x.findall('speaking')]
        while True:
            if chunks and chunks[0].get('quote') == 'true':
                first_chunk = chunks.pop(0)
                self.add_quote(first_chunk.text, 
                            self.meta_info['speakers'][first_chunk.get('speaker')],
                            (turn_seq_no + 0.5))
                continue
            else:
                break
        # our first nonquote turn. pop it from the queue
        first_chunk = chunks.pop(0)
        # first get the name
        current_speaker = self.meta_info['speakers'][first_chunk.get('name')]
        # now initialize the turn
        current_turn = Turn(turn_seq_no,current_speaker,self.document_id)
        # insert turn to db, and add to this object's list
        self.add_turn(current_turn)
        # add text to the turn, which kicks off parses of its sentences
        current_turn.add_text(first_chunk.text)
        # go through the rest
        for chunk in chunks:
            if chunk.get('quote') == 'true':
                #if chunk.get('speaker'):
                q_speaker = chunk.get('speaker')
                #elif chunk.get('name'):
                #    fc_speaker = chunk.get('name')
                self.add_quote(chunk.text, 
                                q_speaker,
                                (turn_seq_no + 0.5))
                continue
            speaker = self.meta_info['speakers'][chunk.get('name')]
            if speaker != current_speaker:
                # end of this turn, so update it to set all text, word etc
                # fields
                self.update_turn(current_turn)
                # increase seq_no
                turn_seq_no += 1
                # set this speaker to the current one
                current_speaker = speaker
                # start a new turn
                current_turn = Turn(turn_seq_no,current_speaker,self.document_id)
                self.add_turn(current_turn)
                current_turn.add_text(chunk.text)
            else:
                current_turn.add_text(chunk.text)
        # end of the last turn, so insert
        self.update_turn(current_turn)
    def extract_date(self):
        year = get_etree_element_text(self.x,'year')
        day = get_etree_element_text(self.x,'day')
        try:
            month = MONTHS[get_etree_element_text(self.x,'month')]
        except KeyError:
            print get_etree_element_text(self.x,'month')
            raise
        if year and month and day:
            return datetime.strptime(year+month+day,'%Y%m%d')
        else:
            return None
    def get_all_speakers(self):
        self.meta_info['speakers'] = {}
        all_speakers = [s.get('name') for s in self.x.findall('speaker')]
        for k in self.x.findall('speaking'):
            if k.get('name') != None:
                all_speakers.append(k.get('name'))
            elif k.get('speaker') != None:
                all_speakers.append(k.get('speaker'))
            else:
                continue
        #all_speakers.extend([k.get('name') for k in self.x.findall('speaking')])
        for speaker_name in all_speakers:
            #speaker_name = speaker.get('name')
            spk = Speaker(speaker_name)
            self.meta_info['speakers'][spk.cr_name] = spk.speaker_id
    def get_meta_info(self):
        self.meta_info = {}
        self.meta_info['volume'] =get_etree_element_text(
                                    self.x,'volume',get_int=True)
        self.meta_info['number'] = get_etree_element_text(
                                    self.x,'number',get_int=True)
        self.meta_info['date'] = self.extract_date()
        self.meta_info['title'] = get_etree_element_text(self.x,'title')
        self.meta_info['congress'] = get_etree_element_text(
                                    self.x,'congress',get_int=True)
        self.meta_info['session'] = get_etree_element_text(
                                    self.x,'session',get_int=True)
        self.meta_info['chamber'] = get_etree_element_text(self.x,'chamber')
        self.meta_info['pages'] = get_etree_element_text(self.x,'pages')
    def insert(self):
        self.document_id = MONGO[DATABASE].documents.insert( {
            'volume': self.meta_info['volume'],
            'number': self.meta_info['number'],
            'date'  : self.meta_info['date'],
            'title'  : self.meta_info['title'],
            'congress'  : self.meta_info['congress'],
            'session'  : self.meta_info['session'],
            'chamber'  : self.meta_info['chamber'],
            'pages' : self.meta_info['pages'],
            'speakers'  : self.meta_info['speakers'].values()})
    def update(self):
        MONGO[DATABASE].documents.update( {'_id':self.document_id},
                {'$set':
                    {   'turn_ids'  : self.turn_ids,
                        'text'      : self.text,
                        'word_count': self.word_count,
                        'word_set'  : self.word_set,
                        'quotes'    : self.quotes}})


def get_etree_element_text(et,element_name,get_int=False):
    element = et.find(element_name)
    if element is not None:
        if get_int:
            return int(element.text)
        else:
            return element.text
    else:
        return None

def get_etree_element_int(et,element_name):
    element = et.find(element_name)
    if element:
        return int(element.text)
    else:
        return None

if __name__ == "__main__":
    CORE_NLP = StanfordCoreNLP()
    xml_directory = sys.argv[1]
    DATABASE = sys.argv[2]
    johnnie = os.walk(xml_directory)
    fnames = []
    for (loc,dirs,files) in os.walk(xml_directory):
        new_fnames = [os.path.join(loc,f) for f in files if f.endswith('.xml')]
        if new_fnames:
            fnames.extend(new_fnames)
    print len(fnames),"xml files will be ingested"
    for fname in fnames:
        res = MONGO[DATABASE].file_list.find_one({'file':fname})
        if res != None:
            if res['xml_error'] == False and res['other_error'] == None:
                continue

        try:
            doc_id = Document(fname).document_id
            xml_error = False
            other_error = None
            error_message = None
        except XMLSyntaxError:
            doc_id = None
            xml_error = True
            other_error = None
            error_message = None
        except Exception, e:
            doc_id = None
            xml_error = False
            other_error = str(type(e))
            error_message = e.message + '\n\nARGS:\n-----\n' + str(e.args)
        finally:
            MONGO[DATABASE].file_list.insert({
                                            "file"          : fname,
                                            "document_id"   : doc_id,
                                            "xml_error"     : xml_error,
                                            "other_error"   : other_error,
                                            "error_message" : error_message })
