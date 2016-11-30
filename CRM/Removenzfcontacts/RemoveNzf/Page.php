<?php
/**
 * @author Jaap Jansma (CiviCooP) <jaap.jansma@civicoop.org>
 * @license http://www.gnu.org/licenses/agpl-3.0.html
 */

class CRM_Removenzfcontacts_RemoveNzf_Page extends CRM_Core_Page {

  public static function remover($ctx, $batch) {
    // Find contacts
    foreach($batch as $contact_id) {
      CRM_Contact_BAO_Contact::deleteContact($contact_id, FALSE, TRUE);
    }
    return true;
  }

  function run() {
    $batchSize = 2;

    $groupId = CRM_Utils_Request::retrieve('group_id', 'Integer', CRM_Core_DAO::$_nullObject, true);
    $sqlParams[1] = array($groupId, 'Integer');

    $queue = CRM_Queue_Service::singleton()->create(array(
      'type' => 'Sql',
      'name' => 'nl.sp.spcustomapi.removenzf',
      'reset' => TRUE, //do not flush queue upon creation
    ));

    $batches = array();
    $contacts = CRM_Core_DAO::executeQuery("
      SELECT civicrm_contact.id 
      FROM civicrm_contact 
      WHERE is_deleted = 0 
      AND civicrm_contact.id IN (
        SELECT contact_id FROM civicrm_group_contact WHERE group_id = %1 AND `status` = 'Added'
      ) 
      AND civicrm_contact.id NOT IN (
        SELECT contact_id FROM civicrm_membership
      );
    ", $sqlParams);
    $i=0;
    $total = 0;
    $batch = array();
    while($contacts->fetch()) {
      $batch[] = $contacts->id;
      $i++;
      $total ++;
      if ($i >= $batchSize) {
        $batches[] = $batch;
        $batch = array();
        $i = 0;
      }
    }
    $batches[] = $batch;

    $i = 0;
    foreach($batches as $batch) {
      $title = ts('Removing contacts %1/%2', array(
        1 => ($i+$batchSize),
        2 => $total,
      ));

      //create a task without parameters
      $task = new CRM_Queue_Task(
        array(
          'CRM_Removenzfcontacts_RemoveNzf_Page',
          'remover'
        ), //call back method
        array($batch), //parameters,
        $title
      );
      //now add this task to the queue
      $queue->createItem($task);
      $i = $i + count($batch);
    }

    $session = CRM_Core_Session::singleton();
    $url = str_replace("&amp;", "&", $session->readUserContext());

    $runner = new CRM_Queue_Runner(array(
      'title' => ts('Removing contacts'), //title fo the queue
      'queue' => $queue, //the queue object
      'errorMode'=> CRM_Queue_Runner::ERROR_ABORT, //abort upon error and keep task in queue
      'onEnd' => array('CRM_SpCustomApi_RemoveNzf_RemoveNzf', 'onEnd'), //method which is called as soon as the queue is finished
      'onEndUrl' => $url,
    ));

    $runner->runAllViaWeb(); // does not return

    parent::run();
  }

  static function onEnd(CRM_Queue_TaskContext $ctx) {
    //set a status message for the user
    CRM_Core_Session::setStatus('Removed contacts', '', 'success');
  }



}