#include <linux/init.h>
#include <linux/version.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/miscdevice.h>
#include <linux/sched.h>
#include <linux/gfp.h>
#include <linux/slab.h>
#include <linux/memory.h>
#include <linux/mm.h>
#include <linux/atomic.h>
#include <paging.h>
#include <linux/list.h>


static unsigned int demand_paging = 1;
module_param(demand_paging, uint, 0644);

struct pageList{
    void * pagePointer; //pointer to page 
    struct list_head pageListHead; //node that will be added to our linked list
};

struct pageInfo{
    struct list_head startNode; //linked list that will be initialized
    atomic_t referenceCount; //reference counter
};


static atomic_t allocCount;
static atomic_t freeCount;


static int do_fault(struct vm_area_struct * vma, unsigned long fault_address)
{
    struct page* newPage;
    struct pageList* pageNode;
    struct pageInfo* currentInfo;
    int updateResult;
    //printk(KERN_INFO "paging_vma_fault() invoked: took a page fault at VA 0x%lx\n", fault_address); 
    newPage = alloc_page(GFP_KERNEL);
    if (!newPage){
        printk("MEMORY ALLOCATION FAILED! \n");
        return VM_FAULT_OOM;
    }
    pageNode = kmalloc(sizeof(struct pageList), GFP_KERNEL);
    currentInfo = vma->vm_private_data;
    pageNode->pagePointer = newPage;
    list_add(&pageNode->pageListHead, &currentInfo->startNode);
    vma->vm_private_data = currentInfo; 
    atomic_add(1,&allocCount); //increment atomic variable for counting new page allocation
    updateResult = remap_pfn_range(vma, PAGE_ALIGN(fault_address), page_to_pfn(newPage), PAGE_SIZE, vma->vm_page_prot);
    if (updateResult<0){
        printk("UPDATING PAGE TABLE FAILED! \n");
        return VM_FAULT_SIGBUS;
    }
    return VM_FAULT_NOPAGE;
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(4,11,0)
static int paging_vma_fault(struct vm_area_struct * vma, struct vm_fault * vmf)
{
    unsigned long fault_address = (unsigned long)vmf->virtual_address
#else
static int paging_vma_fault(struct vm_fault * vmf)

{
    struct vm_area_struct * vma = vmf->vma;
    unsigned long fault_address = (unsigned long)vmf->address;
#endif

    return do_fault(vma, fault_address);
}

static void
paging_vma_open(struct vm_area_struct * vma)
{
    struct pageInfo * currentInfo;
    currentInfo = vma->vm_private_data;
    atomic_add(1,&(currentInfo->referenceCount));
    printk(KERN_INFO "paging_vma_open() invoked\n");
}

static void
paging_vma_close(struct vm_area_struct * vma)
{
    struct pageInfo* currentInfo;
    struct pageList* currentPage;
    currentInfo = vma->vm_private_data;
    atomic_sub(1, &(currentInfo->referenceCount)); //subtractt reference count
    if (atomic_read(&(currentInfo->referenceCount)) <= 0){ //check if 0 (or less just in case)
        list_for_each_entry(currentPage, &currentInfo->startNode, pageListHead){
            __free_page(currentPage->pagePointer); //free page
            atomic_add(1,&freeCount); //Increment atomic variable for counting freed pages
        }
        kfree(currentInfo); //free struct
    }
    printk(KERN_INFO "paging_vma_close() invoked\n");
}

static struct vm_operations_struct
paging_vma_ops = 
{
    .fault = paging_vma_fault,
    .open  = paging_vma_open,
    .close = paging_vma_close
};

static int paging_prepage(struct vm_area_struct * vma){
    struct page* newPage;
    struct pageList* pageNode;
    struct pageInfo* currentInfo;
    unsigned long vmaSize;
    unsigned int numPages;
    int updateResult, i;
    vmaSize = vma->vm_end - vma->vm_start;
    numPages = (int)(vmaSize / PAGE_SIZE);
    currentInfo = vma->vm_private_data;
    for (i=0;i<numPages;i++){
        newPage = alloc_page(GFP_KERNEL);
        if (!newPage){
            printk("MEMORY ALLOCATION FAILED! \n");
            return -ENOMEM;
        }
        pageNode = kmalloc(sizeof(struct pageList), GFP_KERNEL);
        pageNode->pagePointer = newPage;
        list_add(&pageNode->pageListHead, &currentInfo->startNode); 
        atomic_add(1,&allocCount); //increment atomic variable for counting new page allocation
        updateResult = remap_pfn_range(vma, PAGE_ALIGN(vma->vm_start + (PAGE_SIZE * i)), page_to_pfn(newPage), PAGE_SIZE, vma->vm_page_prot); //increment starting address depending on which number page you are allocating
        if (updateResult<0){
            printk("UPDATING PAGE TABLE FAILED! \n");
            return -EFAULT;
        }
    }
    vma->vm_private_data = currentInfo; //updating private data so it contains the list of pointers to each page
    return 0;
}


/* vma is the new virtual address segment for the process */
static int
paging_mmap(struct file           * filp,
            struct vm_area_struct * vma)
{
    struct pageInfo* initialInfo;
    /* prevent Linux from mucking with our VMA (expanding it, merging it 
     * with other VMAs, etc.)
     */
    vma->vm_flags |= VM_IO | VM_DONTCOPY | VM_DONTEXPAND | VM_NORESERVE
              | VM_DONTDUMP | VM_PFNMAP;
    /* setup the vma->vm_ops, so we can catch page faults */
    vma->vm_ops = &paging_vma_ops;

    initialInfo = kmalloc(sizeof(struct pageInfo), GFP_KERNEL);
    atomic_set(&(initialInfo->referenceCount), 0);
    INIT_LIST_HEAD(&(initialInfo->startNode)); //instaniate the list head element
    vma->vm_private_data = initialInfo;
    printk(KERN_INFO "paging_mmap() invoked: new VMA for pid %d from VA 0x%lx to 0x%lx\n",
        current->pid, vma->vm_start, vma->vm_end);
    if (!demand_paging){
        return paging_prepage(vma);
    }
    return 0;
}

static struct file_operations
dev_ops =
{
    .mmap = paging_mmap,
};

static struct miscdevice
dev_handle =
{
    .minor = MISC_DYNAMIC_MINOR,
    .name = PAGING_MODULE_NAME,
    .fops = &dev_ops,
};
/*** END device I/O **/

/*** Kernel module initialization and teardown ***/
static int
kmod_paging_init(void)
{
    int status;

    /* Create a character device to communicate with user-space via file I/O operations */
    status = misc_register(&dev_handle);
    if (status != 0) {
        printk(KERN_ERR "Failed to register misc. device for module\n");
        return status;
    }

    atomic_set(&allocCount, 0); //initialize atomic values to 0.
    atomic_set(&freeCount, 0);

    printk(KERN_INFO "Loaded kmod_paging module\n");

    return 0;
}

static void
kmod_paging_exit(void)
{
    /* Deregister our device file */
    misc_deregister(&dev_handle);
    printk("Page Allocation Count: %d \n", atomic_read(&allocCount));
    printk("Freed Page Count: %d \n", atomic_read(&freeCount));
    printk(KERN_INFO "Unloaded kmod_paging module\n");
}

module_init(kmod_paging_init);
module_exit(kmod_paging_exit);

/* Misc module info */
MODULE_LICENSE("GPL");
