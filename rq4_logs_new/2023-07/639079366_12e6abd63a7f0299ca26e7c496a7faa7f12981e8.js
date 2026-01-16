// frontend/src/components/DeleteReviewConfirmationModal/index.js
import React from 'react';
import { useDispatch } from 'react-redux';
import { deleteReviewThunk } from '../../store/reviews';
import { useModal } from '../../context/Modal';

const DeleteReviewConfirmationModal = ({ review }) => {
  const dispatch = useDispatch();
  const { closeModal } = useModal();

  const handleDelete = () => {
    dispatch(deleteReviewThunk(review.id));
    closeModal();
  };

  const handleCancel = () => {
    closeModal();
  };

  return (
    <div>
      <h2>Confirm Delete</h2>
      <p>Are you sure you want to delete this review?</p>
      <button onClick={handleDelete}>Yes (Delete Review)</button>
      <button onClick={handleCancel}>No (Keep Review)</button>
    </div>
  );
};

export default DeleteReviewConfirmationModal;