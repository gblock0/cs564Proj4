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
    Page*		newPage;

    cout << "haven't done shit" << endl;
    
    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    cout << "after db.openFile" << endl;
    if (status != OK)
    {
      cout << "stats was not OK, but thats good." << endl;
      // file doesn't exist. First create it and allocate
      // an empty header page and data page.
      db.createFile(fileName);
      cout << "after db.createFile" << endl;
      //Create new Header Page
      bufMgr->allocPage(file, hdrPageNo, newPage);
      hdrPage = (FileHdrPage*) newPage;
      cout << "after alloc new Header Page" << endl;
      
      hdrPage->pageCnt = 1;
      cout<<"ima be pissed if this prints"<<endl;
      hdrPage->recCnt = 0;
      
      cout << "should be here" << endl;
        
      //create and initialize new data page
      bufMgr->allocPage(file, newPageNo, newPage);
      cout << "after alloc new page" << endl;
      newPage->init(newPageNo);
      cout << "after init new page" << endl;
      hdrPage->firstPage = newPageNo;
      cout << "after setting hdrPage->firstPage" << endl;
      hdrPage->lastPage = newPageNo;
      cout << "after setting hdrPage->lastPage" << endl;
      hdrPage->pageCnt++;
      cout << "after setting hdrPage->pageCnt" << endl;        
      cout << "after alloc and init new data page" << endl;
    
      //unpin pages from memory
      bufMgr->unPinPage(file, hdrPageNo, true);
      bufMgr->unPinPage(file, newPageNo, true);
        
        
      cout << "after unpinning" << endl;
      
      //flush file to disk
      bufMgr->flushFile(file);
        
        
      cout << "after flushFile" << endl;
    }
    return (FILEEXISTS);
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
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		//read in header page and set HeapFile protected variables headerPage
        //headerPageNo and hdrDirtyFlag
        status = filePtr->getFirstPage(headerPageNo);
        status = filePtr->readPage(headerPageNo, pagePtr);
        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = false;
        
        //read in first data page and set HeapFile protected variables
        status = filePtr->readPage(headerPage->firstPage,pagePtr);
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
    if (status != OK) cerr << "error in unpin of header page\n";
	
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

    if (curPageNo == rid.pageNo){
      curPage->getRecord(rid, rec);
    }else{
      bufMgr->unPinPage(filePtr, rid.pageNo, curDirtyFlag);
      bufMgr->readPage(filePtr, rid.pageNo, curPage);
      curPageNo = rid.pageNo;
      curRec = rid;
      curDirtyFlag = false;
    } 
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    //
    return status;
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

    while(curPageNo != -1)
    {
        tmpRid = curRec;
        status = curPage->nextRecord(tmpRid,nextRid);
        if(status != ENDOFPAGE)
        {
            curPage->getRecord(nextRid, rec);
            if(matchRec(rec))
            {
                curRec = nextRid;
                return status;
            }
        } else //we are at the end of the page, go to next
        {
            curPage->getNextPage(nextPageNo);
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            bufMgr->readPage(filePtr, nextPageNo, curPage);
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = curPage->firstRecord(nextRid);
            if(status == NORECORDS)
            {
                return status;
            }
            curPage->getRecord(nextRid, rec);
            if(matchRec(rec))
            {
                curRec = nextRid;
                outRid = nextRid;
                return status;
            }
        }
    }
    return NORECORDS;
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
    Page*	newPage;
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

    //if they are not equal then we must bring it in to memory
    if(headerPage->lastPage != curPageNo)
    {
        //release old page and bring new one into memory and book keeping
        //what is the point of unpinstatus?
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;
    }
    
    //check to see if there is room on page for record
    status = newPage->insertRecord(rec, rid);
    //if there is not room then we must make a new page
    if(status == NOSPACE)
    {
        newPageNo = (headerPage->lastPage)++;
        bufMgr->allocPage(filePtr, newPageNo, newPage);
        curPage->setNextPage(newPageNo);
        bufMgr->unPinPage(filePtr,curPageNo, true);
        curPageNo = newPageNo;
        curPage = newPage;
        curDirtyFlag = true;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        newPage->init(newPageNo);
        status = newPage->insertRecord(rec, rid);
        //check to see if it fails?
        
    }
  
    curRec = rid;
    outRid = rid;
    
    return status;
}


