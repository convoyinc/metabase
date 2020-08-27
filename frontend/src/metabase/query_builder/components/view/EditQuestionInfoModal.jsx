import React from "react";

import { t } from "ttag";

import Form from "metabase/containers/Form";
import ModalContent from "metabase/components/ModalContent";

import Questions from "metabase/entities/questions";

const EditQuestionInfoModal = ({ question, onClose, onSave }) => (
  <ModalContent title={t`Edit question`} onClose={onClose}>
    <Form
      form={Questions.forms.details_without_collection}
      initialValues={question.card()}
      submitTitle={t`Save`}
      onClose={onClose}
      onSubmit={async card => {
        // if cache_ttl is not an integer, pass it to the API and let it handle the error
        card.cache_ttl = 
          !card.cache_ttl // if blank
            ? null
            : Number.isNaN(parseInt(card.cache_ttl)) // if non-int value
            ? card.cache_ttl
            : parseInt(card.cache_ttl) // else parse int value
        await onSave({ ...question.card(), ...card });
        onClose();
      }}
    />
  </ModalContent>
);

export default EditQuestionInfoModal;
