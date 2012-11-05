#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage = new Page();
    
    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        
        //creat the file 
        status = db.createFile(fileName);
        if(status != OK) { return status; }
        
        //open the file, a good idea!
        status = db.openFile(fileName, file);
        if(status != OK) {return status;}
        
        //Create new Header Page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if(status != OK) return status;
        
        //init hdrPage
        hdrPage = (FileHdrPage*) newPage;
        hdrPage->pageCnt = 0;
        hdrPage->recCnt = 0;
        strncpy(hdrPage->fileName, fileName.c_str(), sizeof(hdrPage->fileName));
        
        //create and initialize new data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        
        if(status != OK) return status;
        newPage->init(newPageNo);
        
        //update hdrPage vars
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        
        //unpin pages from memory
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if(status != OK) return status;
        bufMgr->unPinPage(file, newPageNo, true);
        if(status != OK) return status;
        
        //flush file to disk
        status = bufMgr->flushFile(file);
        if(status != OK) return status;
        
        //close
        status = db.closeFile(file);
        if(status != OK) return status;
        
        return status;
        
    }
    return status;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr; //= new Page();

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        //read in header page and set HeapFile protected variables headerPage
        //headerPageNo and hdrDirtyFlag
        
        //read in first page in file, it is the header page
        status = filePtr->getFirstPage(headerPageNo);
        
        if(status != OK){
            returnStatus = status;
            return;
        }
        
        //with that page number, read in the actual page
        status = bufMgr->readPage(filePtr,headerPageNo,pagePtr);
        if(status != OK){
            returnStatus = status;
            return;
        }
        
        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = false;

        //read in first data page and set HeapFile protected variables
        status = bufMgr->readPage(filePtr, headerPage->firstPage, pagePtr);
        if(status != OK){
            returnStatus = status;
            return;
        }
        curPage = pagePtr;
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
        curRec = NULLRID;
    }
    else
    {
    	cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
    returnStatus = status;
    
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
      curPage = NULL;
      curPageNo = 0;
      curDirtyFlag = false;
      if (status != OK) cerr << "error in unpin of date page\n";
      }
    
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) {
        Error e;
		e.print (status);
    cerr << "error in unpin of header page\n";}
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter
//
//This method returns a record (via the rec structure) given the RID of the record. The private data members curPage and curPageNo should be used to keep track of the current data page pinned in the buffer pool. If the desired record is on the currently pinned page, simply invoke 
//curPage->getRecord(rid, rec) to get the record.  Otherwise, you need to unpin the currently pinned page (assuming a page is pinned) and use the pageNo field of the RID to read the page into the buffer pool.
//
// Going record to record and if one is null then we need to start at the beginning othe bufferpool and eventually get back to the page no we started at

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    //if we already have the page in memory then its easy
    if (curPageNo == rid.pageNo){
        status = curPage->getRecord(rid, rec);
        if (status != OK) 
        {
            return status;
        }
        curRec = rid;
    }else{
        //we need to release our current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            return status;
        }
        //get the needed page
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            return status;
        }
        //set vars for page
        curPageNo = rid.pageNo;
        curRec = rid;
        curDirtyFlag = false;
        
        //now get the actual record
        status = curPage->getRecord(rid, rec);
        if(status != OK) return status;
    }
    
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    //
    return OK;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    //infinite loop on last iteration of scanNext
    //nextPageNo is 32767 not -1
    while(curPageNo != -1)
    {
        //check to see if curPage has a good record
        tmpRid = curRec;
        status = curPage->nextRecord(tmpRid,nextRid);
        // if a next record on the page
        if(status == OK)
        {
            //gets the actual record
            status = curPage->getRecord(nextRid, rec);
            if(status != OK) {
                return status;
            }
            
            //checks if it matches the condition
            curRec = nextRid;
            if(matchRec(rec))
            {
                outRid = nextRid;
                return status;
            }
        } else //we are at the end of the page, go to next
        {
            //gets the next page number
            curPage->getNextPage(nextPageNo);
            //if we are at end of file
            if(nextPageNo == -1){
                return FILEEOF;
            }
            
            //release current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if(status != OK) return status;
            //get next page and update vars
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if(status != OK) return status;
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            
            //gets the first record in the next page
            status = curPage->firstRecord(nextRid);
            //can return NORECORDS if there was no record there,
            //but maybe not at end of file. 
            if(status != NORECORDS){
              //get the actual record
              status = curPage->getRecord(nextRid, rec);
              if(status != OK) {
                return status;
              }


              curRec = nextRid;
              //if matches then YEA!
              if(matchRec(rec))
              {
                outRid = nextRid;
                return status;
              }
            }
        }
    }
    return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{

    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage = new Page();
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;
    
    rid = curRec;
    
    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    //check if last page is currently in memory
    if(curPageNo != headerPage->lastPage)
    {
        //if not then bring it into memory
        
        //release curPage
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinstatus != OK){ 
            return unpinstatus; 
        }
         
        //read in last page and associated book keeping
        status = bufMgr->readPage(filePtr, (headerPage->lastPage), curPage);
        if (status != OK) {return status;}
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
        //update curRecord?
    }
    
    //now we know we have the last page
    //lets see if it is full
    
    status = curPage->insertRecord(rec, rid);
    
    //if that add was successful then we do book keeping and return
    if(status == OK)
    {
        //shoul d we be setting curRec here?
        curRec = rid;
        outRid = rid;
        return OK;
    }
    
    //if status was not OK then page was full and we must allocate new page
    //also do associated book keeping for adding a page
    //alloc and init new page
    status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    if (status != OK){ 
        return status;}
    newPage->init(newPageNo);

    //update new page
    curPage->setNextPage(newPageNo);
    curDirtyFlag = true;
    
    //release curPage
    unpinstatus = bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
    if(unpinstatus != OK) 
    {
    return unpinstatus;
    }
    //read in newly allocated page and update header vars and self vars
    //status = bufMgr->readPage(filePtr, newPageNo, curPage);
    //if(status != OK) return status;
    curPage = newPage;
    curPageNo = newPageNo;
    curDirtyFlag = true;
    headerPage->lastPage = newPageNo;
    headerPage->pageCnt = (headerPage->pageCnt)++;
    hdrDirtyFlag = true;
    
    //now our last page is empty so we should be able to insert it with no probs
    status = curPage->insertRecord(rec, rid);
    if (status != OK) { return status;}
    
    curRec = rid;
    outRid = rid;
    return status;
    
    
    /*
    //if they are not equal then we must bring it in to memory
    if(headerPage->lastPage != curPageNo && status == NOSPACE)
    {
        cout << "in first if" << endl;
        //release old page and bring new one into memory and book keeping
        //what is the point of unpinstatus?
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
    }
    
    //check to see if there is room on page for record
    status = curPage->insertRecord(rec, rid); //WAS newPage->...
    cout << "After second insert" << endl;

    //if there is not room then we must make a new page
    if(status == NOSPACE)
    {
        cout << "No Space making new page" << endl;
        int tempNum = (headerPage->lastPage);
        newPageNo = tempNum++;
        bufMgr->allocPage(filePtr, newPageNo, newPage);
        newPage->init(newPageNo);
        curPage->setNextPage(newPageNo);
        bufMgr->unPinPage(filePtr,curPageNo, curDirtyFlag);
        bufMgr->readPage(filePtr, newPageNo, curPage);
        curPageNo = newPageNo;
        curPage = newPage;
        curDirtyFlag = true;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        status = newPage->insertRecord(rec, rid);
        //check to see if it fails?
        
    }
  
    curRec = rid;
    outRid = rid;
    return status;
     */
}


